"""
edge-telemetry-pipeline — reference pattern for store-and-forward telemetry.

Pattern:  sensor sample -> ring buffer (flash) -> burst upload -> cloud ingest
Guarantees:
  * no sample loss when the link is down (buffered, FIFO, bounded retention)
  * deterministic burst batching to keep radio-on time short (power)
  * idempotent cloud ingest (device_id + seq de-duplication)

This is a generic reference implementation. No employer data, parameters,
customer names or production values appear here.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict, field
from typing import Callable, Iterable

# ----------------------------------------------------------------------------
# 1. Sample
# ----------------------------------------------------------------------------

@dataclass(frozen=True)
class Sample:
    device_id: str
    seq: int                      # monotonic per device -> enables de-duplication
    ts: float                     # epoch seconds, device clock
    payload: dict = field(default_factory=dict)


# ----------------------------------------------------------------------------
# 2. Bounded ring buffer (the "no data loss" half of the pattern)
# ----------------------------------------------------------------------------

class RingBuffer:
    """Fixed-capacity FIFO. Oldest sample is dropped only when full.

    capacity = retention you can defend: N samples, or (rate * retention_s).
    """

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.capacity = capacity
        self._items: list[Sample] = []

    def push(self, sample: Sample) -> bool:
        """Return False if an older sample had to be dropped to make room."""
        dropped = len(self._items) >= self.capacity
        if dropped:
            self._items.pop(0)
        self._items.append(sample)
        return not dropped

    def pop_batch(self, n: int) -> list[Sample]:
        batch, self._items = self._items[:n], self._items[n:]
        return batch

    def __len__(self) -> int:
        return len(self._items)


# ----------------------------------------------------------------------------
# 3. Burst transmitter (BATCH + confirmation token = no double-send)
# ----------------------------------------------------------------------------

class BurstTransmitter:
    """Send in batches, only clear the buffer after the cloud acknowledges.

    Confirmation protocol: POST batch -> server returns {"ack_seq": <highest seq accepted>}
    Anything with seq <= ack_seq is safe to discard.
    """

    def __init__(self, transport: Callable[[list[dict]], dict], batch_size: int = 32,
                 max_retries: int = 3, backoff_s: float = 2.0):
        self.transport = transport
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.backoff_s = backoff_s

    def flush(self, buf: RingBuffer) -> int:
        """Return number of samples confirmed by the server."""
        confirmed = 0
        batch = buf._items[: self.batch_size]
        if not batch:
            return 0
        body = [asdict(s) for s in batch]
        for attempt in range(1, self.max_retries + 1):
            try:
                reply = self.transport(body)
                ack = int(reply["ack_seq"])
                sent = len([s for s in batch if s.seq <= ack])
                buf._items = buf._items[sent:]      # keep un-acked samples
                return sent
            except Exception:
                if attempt == self.max_retries:
                    raise
                time.sleep(self.backoff_s * attempt)   # simple linear backoff
        return confirmed


# ----------------------------------------------------------------------------
# 4. Adaptive sampler (the power <-> resolution trade-off)
# ----------------------------------------------------------------------------

class AdaptiveSampler:
    """Interval scales with battery: low battery -> longer interval.

    interval_s = base_s * multiplier(battery_pct)
    This keeps the device alive without losing the slow-moving signal.
    """

    def __init__(self, base_s: float = 60.0, low_battery_pct: float = 30.0,
                 low_battery_multiplier: float = 4.0):
        self.base_s = base_s
        self.low_battery_pct = low_battery_pct
        self.low_battery_multiplier = low_battery_multiplier

    def interval(self, battery_pct: float) -> float:
        if battery_pct <= self.low_battery_pct:
            return self.base_s * self.low_battery_multiplier
        return self.base_s


# ----------------------------------------------------------------------------
# 5. Cloud ingest (idempotent)
# ----------------------------------------------------------------------------

class Ingest:
    def __init__(self):
        self.seen: set[tuple[str, int]] = set()
        self.stored: list[dict] = []

    def accept(self, batch: Iterable[dict]) -> dict:
        highest = -1
        for rec in batch:
            key = (rec["device_id"], rec["seq"])
            if key not in self.seen:
                self.seen.add(key)
                self.stored.append(rec)
            highest = max(highest, rec["seq"])
        return {"ack_seq": highest}


# ----------------------------------------------------------------------------
# Demo: proves the guarantee that matters — nothing is lost while offline
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    cloud = Ingest()
    online = {"up": False}

    def transport(batch):
        if not online["up"]:
            raise ConnectionError("link down")
        return cloud.accept(batch)

    buf = RingBuffer(capacity=200)
    tx = BurstTransmitter(transport, batch_size=20, max_retries=2, backoff_s=0.01)
    sampler = AdaptiveSampler(base_s=60.0)

    # 100 samples taken with the link down
    for i in range(100):
        buf.push(Sample("edge-01", i, 1_700_000_000 + i, {"battery_pct": 55.0}))
    print(f"buffered while offline: {len(buf)}")

    online["up"] = True
    total = 0
    while len(buf):
        total += tx.flush(buf)
    print(f"confirmed by cloud    : {total}")
    print(f"stored in cloud       : {len(cloud.stored)}")
    print(f"sampling interval at 20% battery: {sampler.interval(20)}s "
          f"vs {sampler.interval(80)}s at 80%")
    assert total == 100 and len(cloud.stored) == 100, "store-and-forward guarantee broken"
    print("OK — zero samples lost")
