import os, random, time, csv
import uuid
from locust import HttpUser, task, between, events
from collections import defaultdict
import urllib.request

# === configure via ENV ===
NUM_KEYS     = int(os.getenv("NUM_KEYS",    "100"))   # small keyspace to force contention
CLUSTER_PROB = float(os.getenv("CLUSTER_PROB","0.8")) # stay on the same key 80% of the time
WRITE_RATIO  = int(os.getenv("WRITE_RATIO", "1"))     # e.g. 1% writes
READ_RATIO   = int(os.getenv("READ_RATIO",  "99"))    #   and 99% reads
NODES       = os.getenv("NODES",
               "http://kv1:8000,http://kv2:8000,http://kv3:8000,http://kv4:8000,http://kv5:8000"
             ).split(",")
LEADER_HOST = os.getenv("LEADER_HOST", "http://kv1:8000")

# global counters and buffers
stale_reads = 0
intervals   = []

# will hold one dict per request
_request_log = []

_WORKER_PID = uuid.uuid4().hex[:8]

# ─── WARM‑UP: run once, only on the master ─────────────────────────
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    # Detect master by class name
    if environment.runner.__class__.__name__ == "MasterRunner":
        print(f"→ WARM-UP: inserting {NUM_KEYS} keys into {LEADER_HOST}")
        for i in range(NUM_KEYS):
            urllib.request.urlopen(f"{LEADER_HOST}/put?key=key{i}&value=0")
        print("✓ Warm-up complete. Starting load test…")

# ─── USER BEHAVIOR ─────────────────────────────────────────────────
class KVUser(HttpUser):
    wait_time = between(0.01, 0.05)
    versions  = defaultdict(float)

    def on_start(self):
        self.current_key = random.randint(0, NUM_KEYS-1)
        print("Starting tests")
        print("NUM_KEYS: ", NUM_KEYS)
        print("CLUSTER_PROB: ", CLUSTER_PROB)
        print("WRITE_RATIO: ", WRITE_RATIO)
        print("READ_RATIO: ", READ_RATIO)

    def next_key(self):
        if random.random() > CLUSTER_PROB:
            self.current_key = random.randint(0, NUM_KEYS-1)
        return f"key{self.current_key}"

    @task(WRITE_RATIO)
    def write(self):
        key   = self.next_key()
        ts_ms = time.time() * 1000
        self.versions[key] = ts_ms

        # ← FIXED: with‑block + catch_response
        with self.client.post(
            f"{LEADER_HOST}/put?key={key}&value={int(ts_ms)}",
            name="/put",
            catch_response=True
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"PUT failed: {resp.status_code}")

    @task(READ_RATIO)
    def read(self):
        global stale_reads, intervals
        key     = self.next_key()
        read_ts = time.time() * 1000
        node    = random.choice(NODES)

        with self.client.get(
            f"{node}/get?key={key}",
            name="/get",
            catch_response=True
        ) as resp:
            if resp.status_code == 200:
                data    = resp.json()
                node_ts = data["timestamp"] / 1e6

                if node_ts < self.versions.get(key, 0):
                    stale_reads += 1
                intervals.append(read_ts - node_ts)
                resp.success()
            elif resp.status_code == 404:
                # key not yet written → treat as a “valid miss”
                resp.success()
            else:
                resp.failure(f"Unexpected status: {resp.status_code}")

# ─── TEARDOWN ──────────────────────────────────────────────────────
@events.quitting.add_listener
def on_quit(environment, **kwargs):
    print(f"Total stale reads = {stale_reads}")
    runner = environment.runner
    suffix = f"_{_WORKER_PID}"
    fname = f"intervals_{WRITE_RATIO}_{READ_RATIO}{suffix}.csv"
    with open(fname, "w") as f:
        f.write("interval_ms\n")
        for iv in intervals:
            f.write(f"{iv}\n")
    print(f"Wrote interval data to {fname}")
    
    # build a per-runner suffix
    runner = environment.runner

    # now include that suffix in the requests-csv name
    req_fname = f"run_{WRITE_RATIO}_{READ_RATIO}_requests{suffix}.csv"
    with open(req_fname, "w") as f:
        f.write("timestamp,type,name,status,response_time,response_length\n")
        for entry in _request_log:
            f.write(
                f"{entry['timestamp']},{entry['type']},{entry['name']},"
                f"{entry['status']},{entry['response_time']},{entry['response_length']}\n"
            )
    print(f"Wrote request log to {req_fname}")


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, context, **kwargs):
    # you can choose to skip failures or include them
    if exception:
        status = "FAIL"
        rt = None
    else:
        status = "OK"
        rt = response_time

    _request_log.append({
        "timestamp": time.time() * 1000,  # ms since epoch
        "type":      request_type,
        "name":      name,
        "status":    status,
        "response_time": rt,
        "response_length": response_length,
    })