# edge-telemetry-pipeline

Reference implementation of **store-and-forward telemetry** for battery-powered
field devices.

```
sensor sample → bounded ring buffer (flash) → burst upload → idempotent cloud ingest
```

## The three guarantees

1. **No sample loss while offline.** Samples are held in a bounded FIFO and only
   removed after the server acknowledges them (`ack_seq`). A failed send never
   clears the buffer.
2. **Bounded retention.** Capacity is explicit, so retention is a decision you can
   state out loud ("N samples / X days at rate R"), not an accident.
3. **Idempotent ingest.** De-duplication on `(device_id, seq)` means a retried burst
   after a lost acknowledgement cannot create duplicate records.

Plus `AdaptiveSampler`: the sampling interval scales with battery, which is the
trade-off between **resolution** and **device life**.

## Run

```bash
python3 telemetry_pipeline.py     # demo: 100 samples buffered offline, 0 lost
python3 run_tests.py              # 4 tests, no third-party dependencies
```

## Where to change things

| Want to | Look at |
|---|---|
| change retention policy | `RingBuffer.capacity` |
| change batch/power behaviour | `BurstTransmitter.batch_size`, `max_retries`, `backoff_s` |
| change the power trade-off curve | `AdaptiveSampler.interval()` |
| swap in a real transport | pass your own callable to `BurstTransmitter` |

Generic reference implementation — no employer data or parameters.
