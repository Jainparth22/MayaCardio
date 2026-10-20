import json
import subprocess
import sys
from pathlib import Path

# Add pipeline to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))
import store

db_path = Path(__file__).parent.parent / "data" / "pipeline.db"
conn = store.init_db(db_path)
sid = "smoke_session_99"

store.upsert_session(conn, sid, 1234567.0)
store.save_samples(conn, sid, 1, 120000, [[720, 1200, 1560]])
store.save_loss(conn, sid, 2, 4000, 4399, "buffer_overflow")
store.save_window(conn, sid, 300, {"window": 300, "complete": True, "magnitude": 12.5}, "offline")

# Test 1: pipeline status
p1 = subprocess.run([sys.executable, "pipeline/cli.py", "status"], capture_output=True, text=True, cwd=str(Path(__file__).parent.parent))
print("STATUS OUT:", p1.stdout.strip())
status_obj = json.loads(p1.stdout.strip())
assert "mode" in status_obj
assert "samples_saved" in status_obj
assert "samples_in_cloud" in status_obj
assert "backlog" in status_obj

# Test 2: pipeline export --session ID --from device
p2 = subprocess.run([sys.executable, "pipeline/cli.py", "export", "--session", sid, "--from", "device"], capture_output=True, text=True, cwd=str(Path(__file__).parent.parent))
print("EXPORT OUT:")
for line in p2.stdout.strip().splitlines():
    print(" ", line)
    obj = json.loads(line)
    assert ("sensor" in obj and "index" in obj and "values" in obj) or ("sensor" in obj and "from_index" in obj and "reason" in obj)

# Test 3: pipeline results --session ID
p3 = subprocess.run([sys.executable, "pipeline/cli.py", "results", "--session", sid], capture_output=True, text=True, cwd=str(Path(__file__).parent.parent))
print("RESULTS OUT:")
for line in p3.stdout.strip().splitlines():
    print(" ", line)
    obj = json.loads(line)
    assert obj.get("window") == 300
    assert obj.get("mode") == "offline"

print("\nALL CLI SMOKE TESTS PASSED!")
