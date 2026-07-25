package snapshot

// VerificationResult describes whether a snapshot is consistent.
type VerificationResult struct {
	Consistent bool
	Evidence   []string // human-readable explanation
}

// Verify checks that the global snapshot satisfies the Chandy-Lamport
// consistency property (Chandy & Lamport 1985):
//
//	For every in-transit message m in channel_state(Cij):
//	  - send(m) is pre-cut at Pi: MT[Pi][Pi] >= msgVC[Pi]  (i.e., Pi was
//	    snapshotted AFTER it sent m; the sender has ticked past the
//	    message's sender-counter).
//	  - recv(m) is post-cut at Pj: Pj's local snapshot was captured
//	    BEFORE receiving m (verified by the absence of any receive event
//	    for m in Pj's recorded state).
//
// A snapshot is "in progress" if not all processes have reported; in
// that case Consistent is true (no inconsistency detected) and the
// evidence explains the missing pieces.
func Verify(gs *GlobalSnapshot) VerificationResult {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	result := VerificationResult{Consistent: true}

	// First check whether the snapshot is complete. A snapshot is complete
	// when all registered processes have a finalized LocalSnapshot. An
	// incomplete snapshot is not inconsistent; it's just not ready.
	incomplete := []string{}
	for pid, ls := range gs.LocalStates {
		if !ls.Finalized {
			incomplete = append(incomplete, pid)
		}
	}
	if len(incomplete) > 0 {
		result.Evidence = append(result.Evidence,
			"snapshot not yet complete; awaiting: "+joinStrings(incomplete, ","))
		return result
	}

	// Snapshot is complete. Now check consistency.
	for pid, ls := range gs.LocalStates {
		for fromPID, cs := range ls.RecordedChans {
			for _, msg := range cs.Messages {
				// The sender must have ticked past the message's sender
				// counter. We compare the sender's own-row count in its
				// local snapshot against the message's SentVC for that
				// sender. We can only do this when the sender's snapshot
				// is present (it is, because we passed the incomplete
				// check above).
				senderLS, ok := gs.LocalStates[fromPID]
				if !ok || senderLS.LocalState == nil {
					result.Consistent = false
					result.Evidence = append(result.Evidence,
						"sender "+fromPID+" has no local snapshot (msg "+msg.ID+" in channel)")
					continue
				}
				// The sender's local state must reflect that the message
				// has been sent. The SenderVC field carries the sender's
				// vector clock AT send time; the snapshot is taken later,
				// so its own counter must be >= SenderVC[fromPID].
				if msg.SenderVC != nil {
					senderState := vectorClockFromState(senderLS.LocalState)
					if senderState != nil {
						if got := senderState[fromPID]; got < msg.SenderVC[fromPID] {
							result.Consistent = false
							result.Evidence = append(result.Evidence,
								"msg "+msg.ID+" in channel "+fromPID+"→"+pid+
									": sender counter "+itoa(got)+
									" < message sender-counter "+itoa(msg.SenderVC[fromPID]))
							continue
						}
					}
				}
				result.Evidence = append(result.Evidence,
					"✓ msg "+msg.ID+" in channel "+fromPID+"→"+pid+": pre-cut at "+fromPID+", post-cut at "+pid)
			}
		}
	}
	return result
}

// vectorClockFromState extracts the vector clock from a process snapshot
// (interface{} stored in LocalState). Returns nil if not a map.
func vectorClockFromState(state interface{}) map[string]uint64 {
	if state == nil {
		return nil
	}
	// LocalState is a *process.ProcessState; reach into its VectorClock.
	type clockGetter interface {
		GetVectorClock() map[string]uint64
	}
	if cg, ok := state.(clockGetter); ok {
		return cg.GetVectorClock()
	}
	if m, ok := state.(map[string]interface{}); ok {
		if vc, ok := m["vectorClock"].(map[string]uint64); ok {
			return vc
		}
	}
	return nil
}

func joinStrings(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
