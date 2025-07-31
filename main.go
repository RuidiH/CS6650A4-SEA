// main.go
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
	svc      = Store{data: make(map[string]Entry)}
	peers    []string
	isLeader bool
	N, R, W  int

	writeDelay = 10 * time.Millisecond
	readDelay  = 5 * time.Millisecond
)

func main() {
	var (
		port    = flag.Int("PORT", 8000, "HTTP port to listen on")
		peerStr = flag.String("PEERS", "", "comma-separated list of peer host:port")
		leader  = flag.Bool("LEADER", false, "set if this node is the leader")
		nFlag   = flag.Int("N", 1, "cluster size")
		rFlag   = flag.Int("R", 1, "read quorum")
		wFlag   = flag.Int("W", 1, "write quorum")
	)
	flag.Parse()

	peers = []string{}
	if *peerStr != "" {
		peers = strings.Split(*peerStr, ",")
	}
	isLeader = *leader
	N, R, W = *nFlag, *rFlag, *wFlag

	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/replicate", replicateHandler)
	http.HandleFunc("/getReplica", getReplicaHandler)
	http.HandleFunc("/config", configHandler)

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

// putHandler handles client writes
func putHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("value")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	ts := time.Now().UnixNano()

	// leader must coordinate writes
	if isLeader {
		// first write locally
		time.Sleep(writeDelay)
		svc.Lock()
		svc.data[key] = Entry{Value: val, Timestamp: ts}
		svc.Unlock()

		// then propagate to peers
		if W == 1 {
			// async fire-and-forget
			for _, p := range peers {
				go replicateTo(p, key, val, ts)
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		// synchronous: wait for at least W-1 acks
		acks := 1
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, p := range peers {
			wg.Add(1)
			go func(peer string) {
				defer wg.Done()
				if replicateTo(peer, key, val, ts) {
					mu.Lock()
					acks++
					mu.Unlock()
				}
			}(p)
		}
		wg.Wait()
		if acks < W {
			http.Error(w, "write quorum not met", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	http.Error(w, "writes only allowed on leader", http.StatusBadRequest)
}

// replicateHandler handles replication RPCs from the leader
func replicateHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("value")
	tsStr := r.URL.Query().Get("timestamp")
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if key == "" || err != nil {
		http.Error(w, "invalid replicate args", http.StatusBadRequest)
		return
	}
	time.Sleep(writeDelay)
	svc.Lock()
	// only overwrite if newer
	if e, ok := svc.data[key]; !ok || ts > e.Timestamp {
		svc.data[key] = Entry{Value: val, Timestamp: ts}
	}
	svc.Unlock()
	w.WriteHeader(http.StatusOK)
}

// getHandler handles client reads
func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	if R == 1 {
		time.Sleep(readDelay)
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

	// read-coordinator: fetch from up to R replicas
	type result struct {
		e  Entry
		ok bool
	}
	resCh := make(chan result, len(peers)+1)
	var wg sync.WaitGroup

	// local first
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(readDelay)
		svc.RLock()
		e, ok := svc.data[key]
		svc.RUnlock()
		resCh <- result{e, ok}
	}()

	// peers next
	for _, p := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s/getReplica?key=%s", peer, key)
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
		}(p)
	}

	// collect
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

	// **Return the winner as JSON, including the timestamp**
	bs, _ := json.Marshal(best)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

// getReplicaHandler returns raw JSON Entry for another node
func getReplicaHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	time.Sleep(readDelay)
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

// replicateTo sends a single replicate RPC
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
