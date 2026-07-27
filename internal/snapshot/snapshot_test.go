package snapshot_test

import (
	"sync"
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/snapshot"
)

// setupCoordinator creates a coordinator with N processes in a full mesh.
func setupCoordinator(t *testing.T, pids []string) (*snapshot.SnapshotCoordinator, map[string][]string) {
	t.Helper()

	var mu sync.Mutex
	markerLog := []string{}

	var coord *snapshot.SnapshotCoordinator

	states := map[string]interface{}{}
	for _, pid := range pids {
		states[pid] = pid + "-state"
	}

	peers := map[string][]string{}
	for _, pid := range pids {
		var ps []string
		for _, other := range pids {
			if other != pid {
				ps = append(ps, other)
			}
		}
		peers[pid] = ps
	}

	coord = snapshot.NewCoordinator(
		func(from, to, snapshotID string) {
			mu.Lock()
			markerLog = append(markerLog, from+"→"+to)
			mu.Unlock()
			// Directly deliver marker (simulating in-process transport).
			// Pass the pre-captured state for the receiving process so the
			// coordinator doesn't need to call onCaptureState internally.
			go coord.OnMarkerReceived(to, from, snapshotID, states[to])
		},
		func(pid string) interface{} {
			return states[pid]
		},
		func(snapshotID string, gs *snapshot.GlobalSnapshot) {
			t.Logf("Snapshot %s complete", snapshotID)
		},
	)

	for _, pid := range pids {
		coord.RegisterProcess(pid, peers[pid])
	}

	_ = markerLog
	return coord, peers
}

func TestSnapshot_3Process_Completes(t *testing.T) {
	pids := []string{"P1", "P2", "P3"}
	coord, _ := setupCoordinator(t, pids)

	snapID, err := coord.InitiateSnapshot("P1")
	if err != nil {
		t.Fatal(err)
	}

	// Wait for completion
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("snapshot %s did not complete in time", snapID)
		default:
			gs := coord.GetSnapshot(snapID)
			if gs != nil && gs.Done() {
				gs.RLockForTest()
				n := len(gs.LocalStates)
				gs.RUnlockForTest()
				if n != 3 {
					t.Fatalf("expected 3 local snapshots, got %d", n)
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestSnapshot_InTransitMessages_Captured tests that RecordMessage properly
// captures in-transit messages on a channel being recorded during a snapshot.
//
// L1 fix: uses 3 processes so that P2 has a recording window on P3→P2 channel.
// Sequence:
//  1. P1 initiates snapshot
//  2. P1's marker arrives at P2 (P2 becomes Participant, starts recording P3→P2)
//  3. A message from P3 is injected via RecordMessage (simulating an in-flight msg)
//  4. P3's marker arrives at P2 — finalises P3→P2 channel
//  5. Verify the message appears in P2's RecordedChans[P3].Messages
func TestSnapshot_InTransitMessages_Captured(t *testing.T) {
	// We need fine control over marker delivery, so build a custom coordinator.
	var coord *snapshot.SnapshotCoordinator
	var mu sync.Mutex
	markerDelivery := make(chan struct{ to, from, sid string }, 16)

	coord = snapshot.NewCoordinator(
		func(from, to, snapshotID string) {
			mu.Lock()
			markerDelivery <- struct{ to, from, sid string }{to, from, snapshotID}
			mu.Unlock()
		},
		func(pid string) interface{} { return pid + "-state" },
		nil,
	)

	coord.RegisterProcess("P1", []string{"P2", "P3"})
	coord.RegisterProcess("P2", []string{"P1", "P3"})
	coord.RegisterProcess("P3", []string{"P1", "P2"})

	snapID, err := coord.InitiateSnapshot("P1")
	if err != nil {
		t.Fatal(err)
	}

	// Drain all P1 marker sends (P1→P2 and P1→P3).
	var markers []struct{ to, from, sid string }
	timeout := time.After(1 * time.Second)
outer:
	for len(markers) < 2 {
		select {
		case m := <-markerDelivery:
			markers = append(markers, m)
		case <-timeout:
			break outer
		}
	}

	// Step 2: deliver P1→P2 marker first (P2 becomes Participant, starts recording P3→P2)
	coord.OnMarkerReceived("P2", "P1", snapID, "P2-state")

	// Step 3: inject an in-transit message from P3 to P2 BEFORE P3's marker arrives.
	// This simulates a message that was sent by P3 before its cut.
	inTransitMsg := &snapshot.Message{
		ID:   "m-intransit",
		From: "P3",
		To:   "P2",
		Data: "in-flight-payload",
	}
	coord.RecordMessage(snapID, "P2", "P3", inTransitMsg)

	// Step 4: deliver remaining markers to complete the snapshot.
	// Deliver P1→P3 marker (P3 becomes Participant)
	coord.OnMarkerReceived("P3", "P1", snapID, "P3-state")
	// P3 sends markers to P1 and P2; drain them.
	for i := 0; i < 4; i++ {
		select {
		case m := <-markerDelivery:
			coord.OnMarkerReceived(m.to, m.from, m.sid, m.to+"-state")
		case <-time.After(500 * time.Millisecond):
		}
	}

	// Wait for snapshot to complete.
	deadline := time.After(3 * time.Second)
	var gs *snapshot.GlobalSnapshot
	for {
		select {
		case <-deadline:
			t.Fatalf("snapshot %s did not complete", snapID)
		default:
			gs = coord.GetSnapshot(snapID)
			if gs != nil && gs.Done() {
				goto done
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

done:
	// Step 5: assert the in-transit message is in P2's channel state from P3.
	gs.RLockForTest()
	defer gs.RUnlockForTest()

	p2snap := gs.LocalStates["P2"]
	if p2snap == nil {
		t.Fatalf("P2 has no local snapshot")
	}
	chanState := p2snap.RecordedChans["P3"]
	if chanState == nil {
		t.Fatalf("P2 has no recorded channel state for P3")
	}
	found := false
	for _, m := range chanState.Messages {
		if m.ID == "m-intransit" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("in-transit message 'm-intransit' not found in P2's channel state from P3 (got %d messages)", len(chanState.Messages))
	}
}

func TestSnapshot_Verify_Consistent(t *testing.T) {
	pids := []string{"P1", "P2", "P3"}
	coord, _ := setupCoordinator(t, pids)

	snapID, _ := coord.InitiateSnapshot("P1")
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("snapshot did not complete")
		default:
			gs := coord.GetSnapshot(snapID)
			if gs != nil && gs.Done() {
				result := snapshot.Verify(gs)
				if !result.Consistent {
					t.Fatalf("snapshot should be consistent: %v", result.Evidence)
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestConcurrentInitiators_TwoSimultaneousSnapshots verifies that two
// snapshots initiated from different processes both complete independently
// with all processes participating.
func TestConcurrentInitiators_TwoSimultaneousSnapshots(t *testing.T) {
	pids := []string{"P1", "P2", "P3"}
	coord, _ := setupCoordinator(t, pids)

	snapID1, err := coord.InitiateSnapshot("P1")
	if err != nil {
		t.Fatal(err)
	}
	snapID2, err := coord.InitiateSnapshot("P2")
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("snapshots did not complete in time")
		default:
			gs1 := coord.GetSnapshot(snapID1)
			gs2 := coord.GetSnapshot(snapID2)
			if gs1 != nil && gs1.Done() && gs2 != nil && gs2.Done() {
				gs1.RLockForTest()
				if len(gs1.LocalStates) != 3 {
					t.Errorf("snapshot 1: expected 3 local states, got %d", len(gs1.LocalStates))
				}
				gs1.RUnlockForTest()
				gs2.RLockForTest()
				if len(gs2.LocalStates) != 3 {
					t.Errorf("snapshot 2: expected 3 local states, got %d", len(gs2.LocalStates))
				}
				gs2.RUnlockForTest()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// deliverAllMarkers drains the marker channel and delivers markers until
// no new markers appear within the grace period. This handles cascading
// markers from processes that become participants.
func deliverAllMarkers(markerCh chan struct{ to, from, sid string }, coord *snapshot.SnapshotCoordinator) {
	const gracePeriod = 100 * time.Millisecond
	for {
		select {
		case m := <-markerCh:
			coord.OnMarkerReceived(m.to, m.from, m.sid, m.to+"-state")
		default:
			// No marker ready — give cascading markers time to arrive
			select {
			case m := <-markerCh:
				coord.OnMarkerReceived(m.to, m.from, m.sid, m.to+"-state")
			case <-time.After(gracePeriod):
				return
			}
		}
	}
}

// TestConcurrentInitiators_MarkerCrossTalk verifies that markers from one
// snapshot don't interfere with another concurrent snapshot's state machine.
// It delivers markers with interleaved snapshot IDs and confirms both complete.
func TestConcurrentInitiators_MarkerCrossTalk(t *testing.T) {
	var coord *snapshot.SnapshotCoordinator
	markerCh := make(chan struct{ to, from, sid string }, 32)

	coord = snapshot.NewCoordinator(
		func(from, to, snapshotID string) {
			markerCh <- struct{ to, from, sid string }{to, from, snapshotID}
		},
		func(pid string) interface{} { return pid + "-state" },
		nil,
	)
	coord.RegisterProcess("P1", []string{"P2", "P3"})
	coord.RegisterProcess("P2", []string{"P1", "P3"})
	coord.RegisterProcess("P3", []string{"P1", "P2"})

	snapA, err := coord.InitiateSnapshot("P1")
	if err != nil {
		t.Fatal(err)
	}
	snapB, err := coord.InitiateSnapshot("P2")
	if err != nil {
		t.Fatal(err)
	}

	deliverAllMarkers(markerCh, coord)

	// Both snapshots should complete independently
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("snapshots did not complete in time")
		default:
			gsA := coord.GetSnapshot(snapA)
			gsB := coord.GetSnapshot(snapB)
			if gsA != nil && gsA.Done() && gsB != nil && gsB.Done() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestConcurrentInitiators_SingleProcessBoth verifies that a single process
// can participate in two concurrent snapshots simultaneously, with independent
// per-snapshot channel recording state.
func TestConcurrentInitiators_SingleProcessBoth(t *testing.T) {
	var coord *snapshot.SnapshotCoordinator
	markerCh := make(chan struct{ to, from, sid string }, 32)

	coord = snapshot.NewCoordinator(
		func(from, to, snapshotID string) {
			markerCh <- struct{ to, from, sid string }{to, from, snapshotID}
		},
		func(pid string) interface{} { return pid + "-state" },
		nil,
	)
	coord.RegisterProcess("P1", []string{"P2", "P3"})
	coord.RegisterProcess("P2", []string{"P1", "P3"})
	coord.RegisterProcess("P3", []string{"P1", "P2"})

	snapA, err := coord.InitiateSnapshot("P1")
	if err != nil {
		t.Fatal(err)
	}
	snapB, err := coord.InitiateSnapshot("P2")
	if err != nil {
		t.Fatal(err)
	}

	deliverAllMarkers(markerCh, coord)

	// Both snapshots should complete
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("snapshots did not complete in time")
		default:
			gsA := coord.GetSnapshot(snapA)
			gsB := coord.GetSnapshot(snapB)
			if gsA != nil && gsA.Done() && gsB != nil && gsB.Done() {
				gsA.RLockForTest()
				if gsA.LocalStates["P3"] == nil {
					t.Error("P3 should have a local snapshot in snapA")
				}
				if len(gsA.LocalStates) != 3 {
					t.Errorf("snapA: expected 3 local states, got %d", len(gsA.LocalStates))
				}
				gsA.RUnlockForTest()
				gsB.RLockForTest()
				if gsB.LocalStates["P3"] == nil {
					t.Error("P3 should have a local snapshot in snapB")
				}
				if len(gsB.LocalStates) != 3 {
					t.Errorf("snapB: expected 3 local states, got %d", len(gsB.LocalStates))
				}
				gsB.RUnlockForTest()

				// Verify P3 had both snapshots active simultaneously
				// by checking it has distinct per-snapshot states
				gsA.RLockForTest()
				p3snapA := gsA.LocalStates["P3"]
				gsA.RUnlockForTest()
				gsB.RLockForTest()
				p3snapB := gsB.LocalStates["P3"]
				gsB.RUnlockForTest()
				if p3snapA != nil && p3snapB != nil {
					if p3snapA.SnapshotID == p3snapB.SnapshotID {
						t.Error("P3 should have different snapshot IDs for the two concurrent snapshots")
					}
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
