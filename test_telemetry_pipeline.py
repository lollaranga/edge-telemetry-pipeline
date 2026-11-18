"""Executable proof of the two guarantees this pattern exists for."""
from telemetry_pipeline import (AdaptiveSampler, BurstTransmitter, Ingest,
                                RingBuffer, Sample)


def _tx(cloud, up):
    def transport(batch):
        if not up["ok"]:
            raise ConnectionError("link down")
        return cloud.accept(batch)
    return transport


def test_no_loss_while_offline_then_recovery():
    cloud, up = Ingest(), {"ok": False}
    buf = RingBuffer(50)
    tx = BurstTransmitter(_tx(cloud, up), batch_size=10, max_retries=1, backoff_s=0)

    for i in range(40):
        buf.push(Sample("d1", i, 1000 + i))
    assert len(buf) == 40

    try:
        tx.flush(buf)
    except ConnectionError:
        pass
    assert len(buf) == 40, "buffer must not be cleared on a failed send"

    up["ok"] = True
    while len(buf):
        tx.flush(buf)
    assert len(cloud.stored) == 40


def test_bounded_retention_drops_oldest_first():
    buf = RingBuffer(3)
    for i in range(5):
        buf.push(Sample("d1", i, 1000 + i))
    assert [s.seq for s in buf._items] == [2, 3, 4]


def test_ingest_is_idempotent():
    cloud = Ingest()
    batch = [{"device_id": "d1", "seq": i, "ts": i} for i in range(3)]
    cloud.accept(batch)
    cloud.accept(batch)                    # duplicate burst after a lost ack
    assert len(cloud.stored) == 3, "duplicates must be dropped by (device_id, seq)"


def test_adaptive_sampler_conserves_power():
    s = AdaptiveSampler(base_s=60, low_battery_pct=30, low_battery_multiplier=4)
    assert s.interval(80) == 60
    assert s.interval(20) == 240
