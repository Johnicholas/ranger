package sql

import (
	"context"
	"database/sql"
	"log"

	rapi "github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/hashicorp/go-multierror"
)

//
// What might a SQL schema for holding some ranje.Ranges look like? Maybe something like this:
//
// CREATE TABLE range (id INTEGER PRIMARY KEY, start TEXT, end TEXT, state TEXT);
// CREATE TABLE child (parentId INTEGER, childId INTEGER, PRIMARY KEY (parentId, childId));
// CREATE TABLE placement (rangeId INTEGER, nodeId TEXT, stateCurrent TEXT, stateDesired TEXT PRIMARY KEY (rangeId, nodeId));
//

type Persister struct {
	// TODO: consider whether a mutex or read-write mutex is necessary here
	db              *sql.DB
	insertRange     *sql.Stmt
	insertChild     *sql.Stmt
	insertPlacement *sql.Stmt
}

// TODO: consider whether a return type that includes a cleanup function,
// where we could call sql.Close, might be appropriate.
// https://github.com/google/wire/blob/main/docs/guide.md#cleanup-functionse
func New(dbConnectionPool *sql.DB) (*Persister, error) {
	var prepareErr error
	insertRange, err := dbConnectionPool.Prepare("INSERT INTO range (id, start, end, state) VALUES (?, ?, ?, ?)")
	if err != nil {
		prepareErr = multierror.Append(prepareErr, err)
	}
	insertChild, err := dbConnectionPool.Prepare("INSERT INTO child (parentId, childId) VALUES (?, ?)")
	if err != nil {
		prepareErr = multierror.Append(prepareErr, err)
	}
	insertPlacement, err := dbConnectionPool.Prepare("INSERT INTO placement (rangeId, nodeId, stateCurrent, stateDesired) VALUES (?, ?, ?, ?)")
	if err != nil {
		prepareErr = multierror.Append(prepareErr, err)
	}
	if prepareErr != nil {
		return nil, prepareErr
	}
	return &Persister{
		db:              dbConnectionPool,
		insertRange:     insertRange,
		insertChild:     insertChild,
		insertPlacement: insertPlacement,
	}, nil
}

// For debugging
func (p *Persister) DumpTables() {
	rows, err := p.db.Query(`SELECT name, sql FROM sqlite_master WHERE type = "table"`)
	if err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var sql string
		if err := rows.Scan(&name, &sql); err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return
		}
		log.Printf("name = %s, sql = %s\n", name, sql)
	}
	if err = rows.Err(); err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return
	}
}

// Public for testability - not part of the Persister interface.
func (p *Persister) GetParents(childId rapi.RangeID) ([]rapi.RangeID, error) {
	var out []rapi.RangeID
	ranges, err := p.db.Query("SELECT parentId FROM child WHERE childId = ?", childId)
	if err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return nil, err
	}
	for ranges.Next() {
		var parent rapi.RangeID
		if err = ranges.Scan(&parent); err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return nil, err
		}
		out = append(out, parent)
	}
	if err = ranges.Err(); err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return nil, err
	}
	return out, nil
}

func (p *Persister) GetChildren(parentId rapi.RangeID) ([]rapi.RangeID, error) {
	var out []rapi.RangeID
	ranges, err := p.db.Query("SELECT childId FROM child WHERE parentId = ?", parentId)
	if err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return nil, err
	}
	for ranges.Next() {
		var childId rapi.RangeID
		if err = ranges.Scan(&childId); err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return nil, err
		}
		out = append(out, childId)
	}
	if err = ranges.Err(); err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return nil, err
	}
	return out, nil
}

// TODO: consider using enumer or something instead of stringer - this is a software maintenance burden.
func parsePlacementStateString(in string) rapi.PlacementState {
	switch in {
	case "PsPending":
		return rapi.PsPending
	case "PsInactive":
		return rapi.PsInactive
	case "PsActive":
		return rapi.PsActive
	case "PsMissing":
		return rapi.PsMissing
	case "PsDropped":
		return rapi.PsDropped
	default:
		return rapi.PsUnknown
	}
}

func (p *Persister) GetPlacements(rangeId rapi.RangeID) ([]*ranje.Placement, error) {
	var out []*ranje.Placement
	ranges, err := p.db.Query("SELECT nodeId, stateCurrent, stateDesired FROM placement WHERE rangeId = ?", rangeId)
	if err != nil {
		log.Println("Maybe the SQL query is malformed?")
		return nil, err
	}
	for ranges.Next() {
		var nodeId string
		var stateCurrentString string
		var stateDesiredString string
		if err = ranges.Scan(&nodeId, &stateCurrentString, &stateDesiredString); err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return nil, err
		}
		out = append(out, &ranje.Placement{
			NodeID:       rapi.NodeID(nodeId),
			StateCurrent: parsePlacementStateString(stateCurrentString),
			StateDesired: parsePlacementStateString(stateDesiredString),
		})
	}
	if err = ranges.Err(); err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return nil, err
	}
	return out, nil
}

// TODO: consider using enumer or something instead of stringer - this is a software maintenance burden.
func parseRangeStateString(in string) rapi.RangeState {
	switch in {
	case "RsActive":
		return rapi.RsActive
	case "RsSubsuming":
		return rapi.RsSubsuming
	case "RsObsolete":
		return rapi.RsObsolete
	default:
		return rapi.RsUnknown
	}
}

func (p *Persister) GetRanges() ([]*ranje.Range, error) {
	out := []*ranje.Range{}
	ranges, err := p.db.Query("SELECT id, start, end, state FROM range")
	if err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return nil, err
	}
	for ranges.Next() {
		// TODO(johnicholas): Consider, should id be a uint64 or an int64?
		// Sqlite3's integer type is (at most general?) a signed 8-byte integer
		// but ranje.Meta id is specified as uint64?
		var idSigned int64
		var start string
		var end string
		var stateString string
		if err = ranges.Scan(&idSigned, &start, &end, &stateString); err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return nil, err
		}
		id := rapi.RangeID(idSigned)
		r := &ranje.Range{
			Meta: rapi.Meta{
				Ident: rapi.RangeID(id),
				Start: rapi.Key(start),
				End:   rapi.Key(end),
			},
			State: parseRangeStateString(stateString),
		}
		out = append(out, r)
	}
	if err = ranges.Err(); err != nil {
		log.Println("Maybe the sql query above is malformed?")
		return nil, err
	}

	for _, r := range out {
		parents, err := p.GetParents(r.Meta.Ident)
		if err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return nil, err
		}
		r.Parents = parents
		children, err := p.GetChildren(r.Meta.Ident)
		if err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return nil, err
		}
		r.Children = children
		placements, err := p.GetPlacements(r.Meta.Ident)
		if err != nil {
			log.Println("Maybe the sql query above is malformed?")
			return nil, err
		}
		r.Placements = placements
	}

	return out, nil
}

func (p *Persister) PutRanges(ranges []*ranje.Range) error {
	// TODO(johnicholas): consider proposing an interface change to pass in a context.Context here
	ctx := context.Background()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// make transaction-specific prepared statements from the existing prepared statements
	insertRange := tx.StmtContext(ctx, p.insertRange)
	insertChild := tx.StmtContext(ctx, p.insertChild)
	insertPlacement := tx.StmtContext(ctx, p.insertPlacement)

	for _, r := range ranges {
		id := r.Meta.Ident              // uint64
		start := r.Meta.Start           // string
		end := r.Meta.End               // string
		stateString := r.State.String() // string

		if _, err := insertRange.ExecContext(ctx, id, start, end, stateString); err != nil {
			log.Println("error in insertRange exec")
			return err
		}

		for _, parentId := range r.Parents {
			if _, err := insertChild.ExecContext(ctx, parentId, id); err != nil {
				log.Println("error in insertChild exec")
				return err
			}
		}
		for _, childId := range r.Children {
			if _, err := insertChild.ExecContext(ctx, id, childId); err != nil {
				log.Println("error in insertChild exec")
				return err
			}
		}
		for _, placement := range r.Placements {
			_, err := insertPlacement.ExecContext(ctx, id, placement.NodeID.String(), placement.StateCurrent.String(), placement.StateDesired.String())
			if err != nil {
				log.Println("error in insertPlacement exec")
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
