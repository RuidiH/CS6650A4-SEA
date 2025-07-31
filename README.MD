
## Intro
The docker-compose.yml hard-code 5 key-value pair nodes. R and W are adjustable in .env file.

SLA Resiliency Testing is not included in this repo.

## Setup
1) Install Docker
2) Install pandas and matplotlib if you have not
3) Rename .env.sample to .env 
4) Update READ_RATIO and WRITE_RATIO for each test

Other parameters you can tune:
 - READ_QUORUM and WRITE_QUORUM -> R and W 
 - USERS and SPAWN_RATE in .env and wait_time in locustfile.py controls the load. Higher USERS and SPAWN_RATE and lower wait_time means more load on your system.
 - Number of locust workers, see below on how to use.
 - writeDelay and readDelay in main.go, default set to A4 requirement.

## Build kv-service image
docker build . -t kv-service

## Load Test with Locust

1) Start Key-Value Service and load tests.
```
$ docker compose up -d --scale locust-worker=4
```
2) Plot results with pandas and matplotlib:
```
python analyze.py
```

You should see aggregatted results for your current read/write ratio (e.g. intervals_1_99.png, read_latency_1_99.png, write_lantency_1_99.png)

## Send requests manually

### POST
curl -i -X POST "http://localhost:8000/put?key=username&value=Alice"

### GET
curl -i "http://localhost:8000/get?key=username"

To observe inconsistency of values across kv nodes, increase the writeDelay (e.g. 5000 ms)

## Results
### Parameters
Defaults in .env 
 - WRITE_QUORUM=4
 - READ_QUORUM=4
 - NUM_KEYS=100
 - WRITE_RATIO=10
 - READ_RATIO=90
 - USERS=25
 - SPAWN_RATE=10
 - RUN_TIME=5m - USERS=25

Defaults in locustfile.py and for docker
 - locust-worker=6 
 - wait_time range between(0.01, 0.05)

### Load Test

1) W/R : 1/99
![](./intervals_1_99.png)
![](./read_latency_1_99.png)
![](./write_latency_1_99.png)

2) W/R : 10/90

3) W/R : 50/50

3) W/R : 90/10