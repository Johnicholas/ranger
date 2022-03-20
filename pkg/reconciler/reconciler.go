package reconciler

import (
	"log"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Reconciler struct {
	ks *ranje.Keyspace
	ch chan roster.NodeInfo
}

func New(ks *ranje.Keyspace) *Reconciler {
	ch := make(chan roster.NodeInfo)
	return &Reconciler{ks, ch}
}

func (r *Reconciler) Chan() chan roster.NodeInfo {
	return r.ch
}

func (r *Reconciler) Run() {
	for nInfo := range r.ch {
		err := r.nodeInfo(nInfo)
		if err != nil {
			log.Printf("err! %v\n", err)
		}
	}
}

type states struct {
	expected *ranje.PBNID
	actual   *roster.RangeInfo
}

func (s *states) hasExpected() bool {
	return s.expected != nil
}

func (s *states) hasActual() bool {
	return s.actual != nil
}

func (r *Reconciler) nodeInfo(nInfo roster.NodeInfo) error {
	states := map[ranje.Ident]states{}

	// Collect the placement we *expect* the node to have.
	for _, pbnid := range r.ks.PlacementsByNodeID(nInfo.NodeID) {
		rID := pbnid.Range.Meta.Ident
		r := states[rID]
		r.expected = &pbnid
		states[rID] = r
	}

	if nInfo.Expired && len(nInfo.Ranges) != 0 {
		panic("expired node with non-zero ranges")
	}

	// Collect the placements that the node reports that it has.
	for _, rInfo := range nInfo.Ranges {
		rID := rInfo.Meta.Ident
		r := states[rID]
		r.actual = &rInfo
		states[rID] = r
	}

	for rID, pair := range states {

		if pair.hasExpected() {
			if pair.hasActual() {

				// Check that remote stat matches local.
				exp := pair.expected.Placement.State
				act := pair.actual.State.ToStatePlacement()
				if exp != act {

					log.Printf(
						"expected range %v on node %v (pos=%d), to be in state %v, but it was in state %v",
						rID, nInfo.NodeID, pair.expected.Position, exp, act)

					// Update local state to match remote.
					err := r.ks.PlacementToState(pair.expected.Placement, act)
					if err != nil {
						log.Printf("error updating state: %v\n", err)
					}
				}
			} else {
				// expected range to be present, but it isn't!
				log.Printf("expected range %v on node %v (pos=%d), but it was missing\n", rID, nInfo.NodeID, pair.expected.Position)

				// Don't worry about it if the *next* placement is missing; we
				// probably asked after it was created here, but hasn't been
				// conveyed to the node yet.
				if pair.expected.Position == 0 {

					// Also don't worry if the placement is SpPending. We've
					// probably just created it controller-side and not told the
					// node about it yet.
					if pair.expected.Placement.State != ranje.SpPending {
						err := r.ks.PlacementToState(pair.expected.Placement, ranje.SpGone)
						if err != nil {
							log.Printf("error updating state: %v\n", err)
						}
					}
				}
			}
		} else {
			if pair.hasActual() {
				// the node reports a placement that we didn't expect.
				log.Printf("got unexpected range %v on node %v\n", rID, nInfo.NodeID)
			} else {
				// Shouldn't be able to reach here. The above is buggy if so.
				panic("expected and actual state are both nil?")
			}
		}
	}

	return nil
}
