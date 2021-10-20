package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	pbr "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	consulapi "github.com/hashicorp/consul/api"
)

type key []byte

// wraps ranger/pkg/proto/gen/Ident
// TODO: move this to the lib
type rangeIdent [40]byte

// TODO: move this to the lib
func parseIdent(pbid *pbr.Ident) (rangeIdent, error) {
	ident := [40]byte{}

	s := []byte(pbid.GetScope())
	if len(s) > 32 {
		return ident, errors.New("invalid range ident: scope too long")
	}

	copy(ident[:], s)
	binary.LittleEndian.PutUint64(ident[32:], pbid.GetKey())

	return rangeIdent(ident), nil
}

// TODO: move this to the lib
func (i rangeIdent) String() string {
	scope, key := i.Decode()

	if scope == "" {
		return fmt.Sprintf("%d", key)
	}

	return fmt.Sprintf("%s/%d", scope, key)
}

// this is only necessary because I made the Dump interface friendly. it would probably be simpler to accept an encoded range ident, or maybe do a better job of hiding the [40]byte
func (i rangeIdent) Decode() (string, uint64) {
	scope := string(bytes.TrimRight(i[:32], "\x00"))
	key := binary.LittleEndian.Uint64(i[32:])
	return scope, key
}

// See also pb.RangeMeta.
type RangeMeta struct {
	ident rangeIdent
	start []byte
	end   []byte
}

func parseRangeMeta(r *pbr.RangeMeta) (RangeMeta, error) {
	ident, err := parseIdent(r.Ident)
	if err != nil {
		return RangeMeta{}, err
	}

	return RangeMeta{
		ident: ident,
		start: r.Start,
		end:   r.End,
	}, nil
}

func (rm *RangeMeta) Contains(k key) bool {
	return ((ranje.Key(rm.start) == ranje.ZeroKey || bytes.Compare(k, rm.start) >= 0) &&
		(ranje.Key(rm.end) == ranje.ZeroKey || bytes.Compare(k, rm.end) < 0))
}

// Doesn't have a mutex, since that probably happens outside, to synchronize with other structures.
type Ranges struct {
	ranges []RangeMeta
}

func NewRanges() Ranges {
	return Ranges{ranges: make([]RangeMeta, 0)}
}

func (rs *Ranges) Add(r RangeMeta) error {
	rs.ranges = append(rs.ranges, r)
	return nil
}

func (rs *Ranges) Remove(ident rangeIdent) {
	idx := -1

	for i := range rs.ranges {
		if rs.ranges[i].ident == ident {
			idx = i
			break
		}
	}

	// This REALLY SHOULD NOT happen, because the ident should have come
	// straight of the range map, and we should still be under the same lock.
	if idx == -1 {
		panic("ident not found in range map")
	}

	// jfc golang
	// https://github.com/golang/go/wiki/SliceTricks#delete-without-preserving-order
	rs.ranges[idx] = rs.ranges[len(rs.ranges)-1]
	rs.ranges = rs.ranges[:len(rs.ranges)-1]
}

func (rs *Ranges) Find(k key) (rangeIdent, bool) {
	for _, rm := range rs.ranges {
		if rm.Contains(k) {
			return rm.ident, true
		}
	}

	return rangeIdent{}, false
}

// This is all specific to the kv example. Nothing generic in here.
type RangeData struct {
	data map[string][]byte

	// TODO: Move this to the rangemeta!!
	state ranje.StateRemote // TODO: guard this
}

func (rd *RangeData) fetchMany(dest RangeMeta, parents []*pbr.Placement) {

	// Parse all the parents before spawning threads. This is fast and failure
	// indicates a bug more than a transient problem.
	rms := make([]*RangeMeta, len(parents))
	for i, p := range parents {
		rm, err := parseRangeMeta(p.Range)
		if err != nil {
			log.Printf("FetchMany failed fast: %s", err)
			rd.state = ranje.StateFetchFailed
			return
		}
		rms[i] = &rm
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mu := sync.Mutex{}

	// Fetch each source range in parallel.
	g, ctx := errgroup.WithContext(ctx)
	for i := range parents {
		// lol, golang
		// https://golang.org/doc/faq#closures_and_goroutines
		i := i

		g.Go(func() error {
			return rd.fetchOne(ctx, &mu, dest, parents[i].Node, rms[i])
		})
	}

	if err := g.Wait(); err != nil {
		rd.state = ranje.StateFetchFailed
		return
	}

	// Can't go straight into rsReady, because that allows writes. The source
	// node(s) are still serving reads, and if we start writing, they'll be
	// wrong. We can only serve reads until the assigner tells them to stop,
	// which will redirect all reads to us. Then we can start writing.
	rd.state = ranje.StateFetched
}

func (rd *RangeData) fetchOne(ctx context.Context, mu *sync.Mutex, dest RangeMeta, addr string, src *RangeMeta) error {
	if addr == "" {
		log.Printf("FetchOne: %s with no addr", src.ident)
		return nil
	}

	log.Printf("FetchOne: %s from: %s", src.ident, addr)

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock())
	if err != nil {
		// TODO: Probably a bit excessive
		log.Fatalf("fail to dial: %v", err)
	}

	client := pbkv.NewKVClient(conn)

	scope, key := src.ident.Decode()
	res, err := client.Dump(ctx, &pbkv.DumpRequest{Range: &pbkv.Ident{Scope: scope, Key: key}})
	if err != nil {
		log.Printf("FetchOne failed: %s from: %s: %s", src.ident, addr, err)

		return err
	}

	// TODO: Optimize loading by including range start and end in the Dump response. If they match, can skip filtering.

	c := 0
	s := 0
	mu.Lock()
	for _, pair := range res.Pairs {

		// TODO: Untangle []byte vs string mess
		if dest.Contains(pair.Key) {
			rd.data[string(pair.Key)] = pair.Value
			c += 1
		} else {
			s += 1
		}
	}
	mu.Unlock()
	log.Printf("Inserted %d keys from range %s via node %s (skipped %d)", c, src.ident, addr, s)

	return nil
}

type Node struct {
	data   map[rangeIdent]*RangeData
	ranges Ranges
	mu     sync.Mutex // guards data and ranges, todo: split into one for ranges, and one for each range in data

	addrLis string
	addrPub string
	srv     *grpc.Server
	disc    discovery.Discoverable
}

// ---- control plane

type nodeServer struct {
	pbr.UnimplementedNodeServer
	node *Node
}

// TODO: most of this can be moved into the lib?
func (n *nodeServer) Give(ctx context.Context, req *pbr.GiveRequest) (*pbr.GiveResponse, error) {
	r := req.Range
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rm, err := parseRangeMeta(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	// TODO: Look in Ranges instead here?
	rd, ok := n.node.data[rm.ident]
	if ok {
		// Special case: We already have this range, but gave up on fetching it.
		// To keep things simple, delete it. it'll be added again (while still
		// holding the lock) below.
		if rd.state == ranje.StateFetchFailed {
			delete(n.node.data, rm.ident)
			n.node.ranges.Remove(rm.ident)
		} else {
			return nil, fmt.Errorf("already have ident: %s", rm.ident)
		}
	}

	rd = &RangeData{
		data:  make(map[string][]byte),
		state: ranje.StateUnknown, // default
	}

	if req.Parents != nil && len(req.Parents) > 0 {
		rd.state = ranje.StateFetching
		rd.fetchMany(rm, req.Parents)

	} else {
		// No current host nor parents. This is a brand new range. We're
		// probably initializing a new empty scope.
		rd.state = ranje.StateReady
	}

	n.node.ranges.Add(rm)
	n.node.data[rm.ident] = rd

	log.Printf("Given: %s", rm.ident)
	return &pbr.GiveResponse{
		State: rd.state.ToProto(),
	}, nil
}

func (s *nodeServer) Serve(ctx context.Context, req *pbr.ServeRequest) (*pbr.ServeResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	if rd.state != ranje.StateFetched && !req.Force {
		return nil, status.Error(codes.Aborted, "won't serve ranges not in the FETCHED state without FORCE")
	}

	rd.state = ranje.StateReady

	log.Printf("Serving: %s", ident)
	return &pbr.ServeResponse{}, nil
}

func (s *nodeServer) Take(ctx context.Context, req *pbr.TakeRequest) (*pbr.TakeResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	if rd.state != ranje.StateReady {
		return nil, status.Error(codes.FailedPrecondition, "can only take ranges in the READY state")
	}

	rd.state = ranje.StateTaken

	log.Printf("Taken: %s", ident)
	return &pbr.TakeResponse{}, nil
}

func (s *nodeServer) Drop(ctx context.Context, req *pbr.DropRequest) (*pbr.DropResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	// Skipping this for now; we'll need to cancel via a context in rd.
	if rd.state == ranje.StateFetching {
		return nil, status.Error(codes.Unimplemented, "dropping ranges in the FETCHING state is not supported yet")
	}

	if rd.state != ranje.StateTaken && !req.Force {
		return nil, status.Error(codes.Aborted, "won't drop ranges not in the TAKEN state without FORCE")
	}

	delete(s.node.data, ident)
	s.node.ranges.Remove(ident)

	log.Printf("Dropped: %s", ident)
	return &pbr.DropResponse{}, nil
}

func (n *nodeServer) Info(ctx context.Context, req *pbr.InfoRequest) (*pbr.InfoResponse, error) {
	res := &pbr.InfoResponse{}

	// lol
	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	// iterate range metadata
	for _, r := range n.node.ranges.ranges {
		scope, key := r.ident.Decode()
		d := n.node.data[r.ident]

		res.Ranges = append(res.Ranges, &pbr.RangeInfo{
			Range: &pbr.RangeMeta{
				Ident: &pbr.Ident{
					Scope: scope,
					Key:   key,
				},
				Start: r.start,
				End:   r.end,
			},
			State: d.state.ToProto(),
			Keys:  uint64(len(d.data)),
		})
	}

	return res, nil
}

// Does not lock range map! You have do to that!
func (s *nodeServer) getRangeData(pbi *pbr.Ident) (rangeIdent, *RangeData, error) {
	if pbi == nil {
		return rangeIdent{}, nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	ident, err := parseIdent(pbi)
	if err != nil {
		return ident, nil, status.Errorf(codes.InvalidArgument, "error parsing range ident: %v", err)
	}

	rd, ok := s.node.data[ident]
	if !ok {
		return ident, nil, status.Error(codes.InvalidArgument, "range not found")
	}

	return ident, rd, nil
}

// ---- data plane

type kvServer struct {
	pbkv.UnimplementedKVServer
	node *Node
}

func (s *kvServer) Dump(ctx context.Context, req *pbkv.DumpRequest) (*pbkv.DumpResponse, error) {
	r := req.Range
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	// TODO: Import the proto properly instead of casting like this!
	ident, err := parseIdent(&pbr.Ident{
		Scope: req.Range.Scope,
		Key:   req.Range.Key,
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range ident: %v", err)
	}

	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	rd, ok := s.node.data[ident]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "range not found")
	}

	if rd.state != ranje.StateTaken {
		return nil, status.Error(codes.FailedPrecondition, "can only dump ranges in the TAKEN state")
	}

	res := &pbkv.DumpResponse{}
	for k, v := range rd.data {
		res.Pairs = append(res.Pairs, &pbkv.Pair{Key: []byte(k), Value: v})
	}

	log.Printf("Dumped: %s", ident)
	return res, nil
}

func (s *kvServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.ranges.Find(key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	if rd.state != ranje.StateReady && rd.state != ranje.StateFetched && rd.state != ranje.StateTaken {
		return nil, status.Error(codes.FailedPrecondition, "can only GET from ranges in the READY, FETCHED, and TAKEN states")
	}

	v, ok := rd.data[k]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such key")
	}

	log.Printf("get %q", k)
	return &pbkv.GetResponse{
		Value: v,
	}, nil
}

func (s *kvServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.ranges.Find(key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	if rd.state != ranje.StateReady {
		return nil, status.Error(codes.FailedPrecondition, "can only PUT to ranges in the READY state")
	}

	if req.Value == nil {
		delete(rd.data, k)
	} else {
		rd.data[k] = req.Value
	}

	log.Printf("put %q", k)
	return &pbkv.PutResponse{}, nil
}

func init() {
	// Ensure that nodeServer implements the NodeServer interface
	var ns *nodeServer = nil
	var _ pbr.NodeServer = ns

	// Ensure that kvServer implements the KVServer interface
	var kvs *kvServer = nil
	var _ pbkv.KVServer = kvs

}

func New(addrLis, addrPub string) (*Node, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	disc, err := consuldisc.New("node", addrPub, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	n := &Node{
		data:   make(map[rangeIdent]*RangeData),
		ranges: NewRanges(),

		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,
	}

	ns := nodeServer{node: n}
	kv := kvServer{node: n}

	pbr.RegisterNodeServer(srv, &ns)
	pbkv.RegisterKVServer(srv, &kv)

	return n, nil
}

func (node *Node) Run(done chan bool) error {

	// For the gRPC server.
	lis, err := net.Listen("tcp", node.addrLis)
	if err != nil {
		return err
	}

	// Start the gRPC server in a background routine.
	errChan := make(chan error)
	go func() {
		err := node.srv.Serve(lis)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	// Register with service discovery
	err = node.disc.Start()
	if err != nil {
		return err
	}

	// Block until channel closes, indicating that caller wants shutdown.
	<-done

	// Let in-flight RPCs finish and then stop. errChan will contain the error
	// returned by server.Serve (above) or be closed with no error.
	node.srv.GracefulStop()
	err = <-errChan
	if err != nil {
		fmt.Printf("Error from server.Serve: ")
		return err
	}

	// Remove ourselves from service discovery. Not strictly necessary, but lets
	// the other nodes respond quicker.
	err = node.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}
