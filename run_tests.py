
"""Run with:  python3 run_tests.py   (no third-party dependencies)"""
import importlib, sys, traceback
mod = importlib.import_module("test_telemetry_pipeline")
fails = passed = 0
for name in sorted(dir(mod)):
    if not name.startswith("test_"):
        continue
    try:
        getattr(mod, name)()
        print(f"PASS  {name}")
        passed += 1
    except Exception:
        print(f"FAIL  {name}")
        traceback.print_exc()
        fails += 1
print(f"\n{passed} passed, {fails} failed")
sys.exit(1 if fails else 0)
