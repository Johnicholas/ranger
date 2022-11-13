package util

import (
	"fmt"

	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

// TODO: Where does this belong? Probably not here!
func GetParents(ks *keyspace.Keyspace, rost *roster.Roster, rang *ranje.Range) []*pb.Parent {
	parents := []*pb.Parent{}
	seen := map[ranje.Ident]struct{}{}
	addParents(ks, rost, rang, &parents, seen)
	return parents
}

func addParents(ks *keyspace.Keyspace, rost *roster.Roster, rang *ranje.Range, parents *[]*pb.Parent, seen map[ranje.Ident]struct{}) {
	_, ok := seen[rang.Meta.Ident]
	if ok {
		return
	}

	*parents = append(*parents, pbPlacement(rost, rang))
	seen[rang.Meta.Ident] = struct{}{}

	for _, rID := range rang.Parents {
		r, err := ks.Get(rID)
		if err != nil {
			// TODO: Think about how to recover from this. It's bad.
			panic(fmt.Sprintf("getting range with ident %v: %v", rID, err))
		}

		addParents(ks, rost, r, parents, seen)
	}
}

func pbPlacement(rost *roster.Roster, r *ranje.Range) *pb.Parent {

	// TODO: The kv example doesn't care about range history, because it has no
	//       external write log, so can only fetch from nodes. So we can skip
	//       sending them at all. Maybe add a controller feature flag?
	//

	pbPlacements := make([]*pb.Placement, len(r.Placements))

	for i, p := range r.Placements {
		n := rost.NodeByIdent(p.NodeID)

		node := ""
		if n != nil {
			node = n.Addr()
		}

		pbPlacements[i] = &pb.Placement{
			Node:  node,
			State: ranje.PlacementStateToProto(p.State),
		}
	}

	return &pb.Parent{
		Range:      ranje.MetaToProto(r.Meta),
		Placements: pbPlacements,
	}
}
