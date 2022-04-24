package balancer

// TODO: This is not just a balancer any more. It's the God Object. Oh dear.

import (
	"context"
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type debugServer struct {
	pb.UnsafeDebugServer
	bal *Balancer
}

func rangeResponse(r *ranje.Range) *pb.RangeResponse {
	res := &pb.RangeResponse{
		Meta:  r.Meta.ToProto(),
		State: r.State.ToProto(),
	}

	for _, p := range r.Placements {
		res.Placements = append(res.Placements, &pb.Placement{
			Node:  p.NodeID,
			State: p.State.ToProto(),
		})
	}

	return res
}

func nodeResponse(ks *ranje.Keyspace, n *roster.Node) *pb.NodeResponse {
	res := &pb.NodeResponse{
		Node: &pb.NodeMeta{
			Ident:     n.Ident(),
			Address:   n.Addr(),
			WantDrain: n.WantDrain(),
		},
	}

	for _, pl := range ks.PlacementsByNodeID(n.Ident()) {
		res.Ranges = append(res.Ranges, &pb.NodeRange{
			Meta:  pl.Range.Meta.ToProto(),
			State: pl.Placement.State.ToProto(),
		})
	}

	return res
}

func (srv *debugServer) RangesList(ctx context.Context, req *pb.RangesListRequest) (*pb.RangesListResponse, error) {
	ks := srv.bal.ks.DangerousDebuggingMethods()
	res := &pb.RangesListResponse{}

	ranges, unlocker := ks.Ranges()
	defer unlocker()

	for _, r := range ranges {
		r.Mutex.Lock()
		res.Ranges = append(res.Ranges, rangeResponse(r))
		r.Mutex.Unlock()
	}

	return res, nil
}

func (srv *debugServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	if req.Range == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("IdentFromProto failed: %v", err))
	}

	r, err := srv.bal.ks.Get(rID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("GetByIdent failed: %v", err))
	}

	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	res := rangeResponse(r)

	return res, nil
}

func (srv *debugServer) Node(ctx context.Context, req *pb.NodeRequest) (*pb.NodeResponse, error) {
	nID := req.Node
	if nID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: node")
	}

	node := srv.bal.rost.NodeByIdent(nID)
	if node == nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("No such node: %s", nID))
	}

	res := nodeResponse(srv.bal.ks, node)

	return res, nil
}

func (srv *debugServer) NodesList(ctx context.Context, req *pb.NodesListRequest) (*pb.NodesListResponse, error) {
	rost := srv.bal.rost
	rost.RLock()
	defer rost.RUnlock()

	res := &pb.NodesListResponse{}

	for _, n := range rost.Nodes {
		res.Nodes = append(res.Nodes, nodeResponse(srv.bal.ks, n))
	}

	return res, nil
}
