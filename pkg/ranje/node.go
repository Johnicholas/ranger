package ranje

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

const (
	staleTimer  = 10 * time.Second
	giveTimeout = 3 * time.Second
)

// TODO: Add the ident in here?
type Node struct {
	host string
	port int

	// when was this created? needed to drop nodes which never connect.
	init time.Time

	// time last seen in service discovery.
	seen time.Time

	conn   *grpc.ClientConn
	client pb.NodeClient
	muConn sync.RWMutex

	// The ranges that this node has. Populated via Probe.
	ranges   map[Ident]*Placement
	muRanges sync.RWMutex

	// TODO: Figure out what to do with these. They shouldn't exist, and indicate a state bug. But ignoring them probably isn't right.
	unexpectedRanges map[Ident]*pb.RangeMeta
}

func NewNode(host string, port int) *Node {
	n := Node{
		host:             host,
		port:             port,
		init:             time.Now(),
		seen:             time.Time{}, // never
		ranges:           map[Ident]*Placement{},
		unexpectedRanges: map[Ident]*pb.RangeMeta{},
	}

	// start dialling in background
	//zap.L().Info("dialing...", zap.String("addr", n.addr))
	// todo: inherit context to allow global cancellation
	conn, err := grpc.DialContext(context.Background(), fmt.Sprintf("%s:%d", n.host, n.port), grpc.WithInsecure())
	if err != nil {
		//zap.L().Info("error while dialing", zap.String("addr", n.addr), zap.Error(err))
		fmt.Printf("error while dialing: %v\n", err)
	}

	n.muConn.Lock()
	n.conn = conn
	n.client = pb.NewNodeClient(n.conn)
	n.muConn.Unlock()

	return &n
}

func (n *Node) String() string {
	return fmt.Sprintf("N{%s}", n.addr())
}

// TODO: Replace this with a statusz-type page
func (n *Node) DumpForDebug() {
	for id, p := range n.ranges {
		fmt.Printf("   - %s %s\n", id.String(), p.state.String())
	}
}

// Seen tells us that the node is still in service discovery.
func (n *Node) Seen(t time.Time) {
	n.seen = t
}

func (n *Node) IsStale(now time.Time) bool {
	return n.seen.Before(now.Add(-staleTimer))
}

func (n *Node) addr() string {
	return fmt.Sprintf("%s:%d", n.host, n.port)
}

func (n *Node) Give(id Ident, r *Range) error {
	// TODO: Is there any point in this?
	_, ok := n.ranges[id]
	if ok {
		// Note that this doesn't check the *other* nodes, only this one
		return fmt.Errorf("range already given to node %s: %s", n.addr(), id.String())
	}

	pp, err := NewPlacement(r, n)
	if err != nil {
		return fmt.Errorf("couldn't Give range; error creating placement: %s", err)
	}

	rm := r.Meta.ToProto()

	// Build a list of other nodes that currently have this range.
	// TODO: Something about remote state here, not all are valid.
	parents := []*pb.RangeNode{}
	for _, p := range r.placements {
		if p.state != SpTaken {
			panic("can't give range when non-taken placements exist!")
		}
		parents = append(parents, &pb.RangeNode{
			Range: rm,
			Node:  p.Addr(),
		})
	}

	req := &pb.GiveRequest{
		Range:   rm,
		Parents: parents,
	}

	// TODO: Move outside?
	ctx, cancel := context.WithTimeout(context.Background(), giveTimeout)
	defer cancel()

	res, err := n.client.Give(ctx, req)
	if err != nil {
		return err
	}

	rs := RemoteStateFromProto(res.State)
	if rs == StateReady {
		pp.ToState(SpReady)

	} else if rs == StateFetching {
		pp.ToState(SpFetching)

	} else if rs == StateFetched {
		// The fetch finished before the client returned.
		pp.ToState(SpFetching)
		pp.ToState(SpFetched)

	} else if rs == StateFetchFailed {
		// The fetch failed before the client returned.
		pp.ToState(SpFetching)
		pp.ToState(SpFetchFailed)

	} else {
		// Got either Unknown or Taken
		panic(fmt.Sprintf("unexpected remote state from Give: %s", rs.String()))
	}

	return nil
}

// Probe updates current state of the node via RPC.
// Returns error if the RPC fails or if a probe is already in progess.
func (n *Node) Probe(ctx context.Context) error {
	// TODO: Abort if probe in progress.

	res, err := n.client.Info(ctx, &pb.InfoRequest{})
	if err != nil {
		fmt.Printf("Probe failed: %s\n", err)
		return err
	}

	for _, r := range res.Ranges {
		rr := r.Range
		if rr == nil {
			fmt.Printf("Malformed probe response from node %s: Range is nil\n", n.addr())
			continue
		}

		id, err := IdentFromProto(rr.Ident)
		if err != nil {
			fmt.Printf("Got malformed ident from node %s: %s\n", n.addr(), err.Error())
			continue
		}

		rrr, ok := n.ranges[*id]

		if !ok {
			fmt.Printf("Got unexpected range from node %s: %s\n", n.addr(), id.String())
			n.unexpectedRanges[*id] = rr
			continue
		}

		// TODO: We compare the Ident here even though we just fetched the assignment by ID. Is that... why
		if !rrr.rang.SameMeta(*id, rr.Start, rr.End) {
			fmt.Printf("Remote range did not match local range with same ident: %s\n", id.String())
			continue
		}

		// Finally update the remote info
		rrr.K = r.Keys

		// TODO: Figure out wtf to do when remote state doesn't match local
		//rrr.state = RemoteStateFromProto(r.State)
	}

	return nil
}

// Drop cleans up the node. Called when it hasn't responded to probes in a long time.
func (n *Node) Drop() {
	n.muConn.Lock()
	defer n.muConn.Unlock()
	n.conn.Close()
}

func (n *Node) Conn() (grpc.ClientConnInterface, error) {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	if n.conn == nil {
		return nil, errors.New("tried to read nil connection")
	}
	return n.conn, nil
}

func (n *Node) Connected() bool {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	return n.conn != nil
}
