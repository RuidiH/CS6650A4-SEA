package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

type Store struct {
	sync.RWMutex
	data map[string]Entry
}

var (
	svc                      = Store{data: make(map[string]Entry)}
	peers                    []string
	isLeader                 bool
	N, R, W                  int
	LeaderDelayPerFollower   = 200 * time.Millisecond
	FollowerUpdateSleep      = 100 * time.Millisecond
	FollowerSleepOnLeaderRead = 50 * time.Millisecond
)

func main() {
	port := flag.Int("PORT", 8000, "HTTP port to listen on")
	peerStr := flag.String("PEERS", "", "comma-separated list of peer host:port")
	leader := flag.Bool("LEADER", false, "set if this node is the leader")
	nFlag := flag.Int("N", 1, "cluster size")
	rFlag := flag.Int("R", 1, "read quorum")
	wFlag := flag.Int("W", 1, "write quorum")
	flag.Parse()

	if *peerStr != "" {
		peers = strings.Split(*peerStr, ",")
	}
	isLeader = *leader
	N, R, W = *nFlag, *rFlag, *wFlag

	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/replicate", replicateHandler)
	http.HandleFunc("/getReplica", getReplicaHandler)
	http.HandleFunc("/config", configHandler)
	http.HandleFunc("/local_read", localReadHandler)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("starting KV service on %s (leader=%v N=%d W=%d R=%d peers=%v)",
		addr, isLeader, N, W, R, peers)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func configHandler(w http.ResponseWriter, r *http.Request) {
	if v := r.URL.Query().Get("N"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			N = i
		}
	}
	if v := r.URL.Query().Get("W"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			W = i
		}
	}
	if v := r.URL.Query().Get("R"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			R = i
		}
	}
	fmt.Fprintf(w, "reconfigured to N=%d W=%d R=%d\n", N, W, R)
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("value")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	ts := time.Now().UnixNano()

	// --- Leader writes ---
	if isLeader {
		// local write
		svc.Lock()
		svc.data[key] = Entry{Value: val, Timestamp: ts}
		svc.Unlock()

		// W=1: fire‐and‐forget, simulate 200ms hardware delay in each goroutine
		if W == 1 {
			for _, peer := range peers {
				go func(p string) {
					time.Sleep(LeaderDelayPerFollower)
					replicateTo(p, key, val, ts)
				}(peer)
			}
			w.WriteHeader(http.StatusCreated)
			return
		}

		// W>1: synchronous, sequential with delay, stop once W acks
		acks := 1
		for _, peer := range peers {
			time.Sleep(LeaderDelayPerFollower)
			if replicateTo(peer, key, val, ts) {
				acks++
			}
			if acks >= W {
				break
			}
		}
		if acks < W {
			http.Error(w, "write quorum not met", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	}

	// --- Leaderless mode: any node can coordinate if W==N ---
	if !isLeader && W == N {
		// local write
		svc.Lock()
		svc.data[key] = Entry{Value: val, Timestamp: ts}
		svc.Unlock()

		acks := 1
		for _, peer := range peers {
			time.Sleep(LeaderDelayPerFollower)
			if replicateTo(peer, key, val, ts) {
				acks++
			}
		}
		if acks < W {
			http.Error(w, "write quorum not met", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	}

	http.Error(w, "writes only allowed on leader", http.StatusBadRequest)
}

func replicateHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("value")
	tsStr := r.URL.Query().Get("timestamp")
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if key == "" || err != nil {
		http.Error(w, "invalid replicate args", http.StatusBadRequest)
		return
	}

	time.Sleep(FollowerUpdateSleep)
	svc.Lock()
	if e, ok := svc.data[key]; !ok || ts > e.Timestamp {
		svc.data[key] = Entry{Value: val, Timestamp: ts}
	}
	svc.Unlock()

	w.WriteHeader(http.StatusOK)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	// R=1: local-only read
	if R == 1 {
		svc.RLock()
		e, ok := svc.data[key]
		svc.RUnlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		bs, _ := json.Marshal(e)
		w.Header().Set("Content-Type", "application/json")
		w.Write(bs)
		return
	}

	// R>1: read‐coordinator fetches from up to R replicas
	type result struct {
		e  Entry
		ok bool
	}
	resCh := make(chan result, len(peers)+1)
	var wg sync.WaitGroup

	// local read
	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.RLock()
		e, ok := svc.data[key]
		svc.RUnlock()
		resCh <- result{e, ok}
	}()

	// peer reads via /getReplica
	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s/getReplica?key=%s", p, key)
			resp, err := http.Get(url)
			if err != nil {
				resCh <- result{Entry{}, false}
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				resCh <- result{Entry{}, false}
				return
			}
			var e Entry
			json.NewDecoder(resp.Body).Decode(&e)
			resCh <- result{e, true}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	got := 0
	var best Entry
	for r2 := range resCh {
		if !r2.ok {
			continue
		}
		got++
		if r2.e.Timestamp > best.Timestamp {
			best = r2.e
		}
		if got >= R {
			break
		}
	}
	if got < 1 {
		http.NotFound(w, r)
		return
	}

	bs, _ := json.Marshal(best)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func getReplicaHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	// simulate follower‐read delay from leader
	time.Sleep(FollowerSleepOnLeaderRead)

	svc.RLock()
	e, ok := svc.data[key]
	svc.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	bs, _ := json.Marshal(e)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func replicateTo(peer, key, val string, ts int64) bool {
	url := fmt.Sprintf("http://%s/replicate?key=%s&value=%s&timestamp=%d",
		peer, key, val, ts)
	resp, err := http.Post(url, "", nil)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// localReadHandler returns this node’s in‐memory value without any delay
func localReadHandler(w http.ResponseWriter, r *http.Request) {
   key := r.URL.Query().Get("key")
   svc.RLock()
   e, ok := svc.data[key]
   svc.RUnlock()
   if !ok {
       http.NotFound(w, r)
       return
   }
   bs, _ := json.Marshal(e)
   w.Header().Set("Content-Type", "application/json")
   w.Write(bs)
}