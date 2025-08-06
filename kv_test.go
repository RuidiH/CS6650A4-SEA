package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// binName will hold the full path to the built server binary
var binName string

// TestMain builds the server once into binName
func TestMain(m *testing.M) {
	// pick .exe on Windows
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "⚠️ cannot get working dir: %v\n", err)
		os.Exit(1)
	}
	binName = filepath.Join(wd, "kvserver_test_bin"+ext)

	// build main.go → binName
	build := exec.Command("go", "build", "-o", binName, "main.go")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "⚠️ failed to build server: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	os.Remove(binName)
	os.Exit(code)
}

// startNode launches one server instance
func startNode(t *testing.T, port int, peers []string, leader bool, N, R, W int) *exec.Cmd {
	args := []string{
		"-PORT", fmt.Sprint(port),
		"-PEERS", strings.Join(peers, ","),
		"-N", fmt.Sprint(N),
		"-R", fmt.Sprint(R),
		"-W", fmt.Sprint(W),
	}
	if leader {
		args = append(args, "-LEADER")
	}
	cmd := exec.Command(binName, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node on port %d: %v", port, err)
	}
	return cmd
}

// getEntry does an HTTP GET and unmarshals an Entry if 200 OK
func getEntry(t *testing.T, url string) (Entry, int) {
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Entry{}, resp.StatusCode
	}
	b, _ := io.ReadAll(resp.Body)
	var e Entry
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("unmarshal %s: %v", url, err)
	}
	return e, resp.StatusCode
}

func TestLeader_ImmediateConsistencyAndWindow(t *testing.T) {
	leaderPort, f1Port, f2Port := 9001, 9002, 9003
	peerAddrs := []string{
		fmt.Sprintf("localhost:%d", f1Port),
		fmt.Sprintf("localhost:%d", f2Port),
	}

	// start leader + 2 followers with N=3, W=3, R=1
	leader := startNode(t, leaderPort, peerAddrs, true, 3, 1, 3)
	f1 := startNode(t, f1Port, []string{
		fmt.Sprintf("localhost:%d", leaderPort),
		fmt.Sprintf("localhost:%d", f2Port),
	}, false, 3, 1, 3)
	f2 := startNode(t, f2Port, []string{
		fmt.Sprintf("localhost:%d", leaderPort),
		fmt.Sprintf("localhost:%d", f1Port),
	}, false, 3, 1, 3)
	defer leader.Process.Kill()
	defer f1.Process.Kill()
	defer f2.Process.Kill()

	time.Sleep(200 * time.Millisecond)

	key, val := "foo", "bar"
	setURL := fmt.Sprintf("http://localhost:%d/set?key=%s&value=%s", leaderPort, key, val)

	// 1) launch the write asynchronously
	writeDone := make(chan *http.Response, 1)
	go func() {
		resp, _ := http.Post(setURL, "", nil)
		writeDone <- resp
	}()

	// 2) wait ~100ms, then probe follower2's /local_read
	time.Sleep(100 * time.Millisecond)
	_, code := getEntry(t, fmt.Sprintf("http://localhost:%d/local_read?key=%s", f2Port, key))
	if code == http.StatusOK {
		t.Errorf("expected follower2 to still be stale, but /local_read returned OK")
	}

	// 3) now wait for the write to finish and assert 201
	resp := <-writeDone
	if resp == nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created from leader, got %v", resp)
	}

	// 4) after the write completes, reads from leader and follower1 should succeed
	eL, code := getEntry(t, fmt.Sprintf("http://localhost:%d/get?key=%s", leaderPort, key))
	if code != http.StatusOK || eL.Value != val {
		t.Errorf("leader /get: expected %q got %q (code %d)", val, eL.Value, code)
	}
	eF1, code := getEntry(t, fmt.Sprintf("http://localhost:%d/get?key=%s", f1Port, key))
	if code != http.StatusOK || eF1.Value != val {
		t.Errorf("follower1 /get: expected %q got %q (code %d)", val, eF1.Value, code)
	}
}

func TestLeaderless_InconsistencyWindowThenConsistency(t *testing.T) {
	p1, p2, p3 := 9011, 9012, 9013
	allPeers := []string{
		fmt.Sprintf("localhost:%d", p1),
		fmt.Sprintf("localhost:%d", p2),
		fmt.Sprintf("localhost:%d", p3),
	}

	// start 3 nodes, none a leader, W=N=3, R=1
	n1 := startNode(t, p1, []string{allPeers[1], allPeers[2]}, false, 3, 1, 3)
	n2 := startNode(t, p2, []string{allPeers[0], allPeers[2]}, false, 3, 1, 3)
	n3 := startNode(t, p3, []string{allPeers[0], allPeers[1]}, false, 3, 1, 3)
	defer n1.Process.Kill()
	defer n2.Process.Kill()
	defer n3.Process.Kill()
	time.Sleep(200 * time.Millisecond)

	key, val := "baz", "qux"
	setURL := fmt.Sprintf("http://localhost:%d/set?key=%s&value=%s", p1, key, val)

    // 1) launch the write asynchronously (p1 is coordinator)
    writeDone := make(chan *http.Response, 1)
    go func() {
        resp, _ := http.Post(setURL, "", nil)
        writeDone <- resp
    }()

    // 2) wait ~100ms, then probe follower2’s local_read before it’s updated
    time.Sleep(100 * time.Millisecond)
    _, code := getEntry(t, fmt.Sprintf("http://localhost:%d/local_read?key=%s", p2, key))
    if code == http.StatusOK {
        t.Errorf("expected p2 to still be stale during window, but /local_read returned OK")
    }

    // 3) now wait for the write to finish and assert 201 Created
    resp := <-writeDone
    if resp == nil || resp.StatusCode != http.StatusCreated {
        t.Fatalf("expected 201 Created from p1, got %v", resp)
    }

    // 4) after the write completes, reads from coordinator and a follower should succeed
    e1, code := getEntry(t, fmt.Sprintf("http://localhost:%d/get?key=%s", p1, key))
    if code != http.StatusOK || e1.Value != val {
        t.Errorf("p1 /get: expected %q got %q (code %d)", val, e1.Value, code)
    }
    e2, code := getEntry(t, fmt.Sprintf("http://localhost:%d/get?key=%s", p2, key))
    if code != http.StatusOK || e2.Value != val {
        t.Errorf("p2 /get: expected %q got %q (code %d)", val, e2.Value, code)
    }
}
