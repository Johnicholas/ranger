package sql_test

import (
	"database/sql"
	"log"
	"os"
	"testing"

	"github.com/adammck/ranger/pkg/api"
	persisterSQL "github.com/adammck/ranger/pkg/persister/sql"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	_ "modernc.org/sqlite"
)

func init() {
	log.SetOutput(os.Stdout)                     // DEBUG
	log.SetFlags(log.LstdFlags | log.Lshortfile) // DEBUG
}

// Returns a fresh, memory-backed, sqlite-based, range database
func freshTestDB() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS range (id INTEGER PRIMARY KEY, start TEXT, end TEXT, state TEXT)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS child (parentId INTEGER, childId INTEGER, PRIMARY KEY (parentId, childId))")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS placement (rangeId INTEGER, nodeId TEXT, stateCurrent TEXT, stateDesired TEXT, PRIMARY KEY (rangeId, nodeId))")
	if err != nil {
		panic(err)
	}
	return db
}

func TestGetNothing(t *testing.T) {
	// Arrange
	db := freshTestDB()
	defer db.Close()
	systemUnderTest, err := persisterSQL.New(db)
	if err != nil {
		t.Error(err)
		return
	}

	// Act
	got, err := systemUnderTest.GetRanges()
	if err != nil {
		t.Error(err)
		return
	}

	// Assert
	want := []*ranje.Range{}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetRanges() mismatch (-want +got):\n%s", diff)
	}
}

func TestPutNothingGetNothing(t *testing.T) {
	// Arrange
	db := freshTestDB()
	defer db.Close()
	systemUnderTest, err := persisterSQL.New(db)
	if err != nil {
		t.Error(err)
		return
	}

	// Act
	err = systemUnderTest.PutRanges([]*ranje.Range{})
	if err != nil {
		t.Error(err)
		return
	}
	got, err := systemUnderTest.GetRanges()
	if err != nil {
		t.Error(err)
		return
	}

	// Assert
	want := []*ranje.Range{}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetRanges() mismatch (-want +got):\n%s", diff)
	}
}

func TestPutSomethingGetParents(t *testing.T) {
	// Arrange
	db := freshTestDB()
	defer db.Close()
	systemUnderTest, err := persisterSQL.New(db)
	if err != nil {
		t.Error(err)
		return
	}

	// Act
	a := ranje.NewRange(api.RangeID(1234), &ranje.ReplicationConfig{
		TargetActive:  0,
		MinActive:     0,
		MaxActive:     0,
		MinPlacements: 0,
		MaxPlacements: 0,
	})
	b := ranje.NewRange(api.RangeID(5678), &ranje.ReplicationConfig{
		TargetActive:  0,
		MinActive:     0,
		MaxActive:     0,
		MinPlacements: 0,
		MaxPlacements: 0,
	})
	b.Parents = []api.RangeID{a.Meta.Ident}

	err = systemUnderTest.PutRanges([]*ranje.Range{a, b})
	if err != nil {
		t.Error(err)
		return
	}

	got, err := systemUnderTest.GetParents(b.Meta.Ident)
	if err != nil {
		t.Error(err)
		return
	}

	// Assert
	want := []api.RangeID{a.Meta.Ident}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetParents() mismatch (-want +got):\n%s", diff)
	}
}

func TestPutSomethingGetChildren(t *testing.T) {
	// Arrange
	db := freshTestDB()
	defer db.Close()
	systemUnderTest, err := persisterSQL.New(db)
	if err != nil {
		t.Error(err)
		return
	}
	a := ranje.NewRange(api.RangeID(1234), &ranje.ReplicationConfig{
		TargetActive:  0,
		MinActive:     0,
		MaxActive:     0,
		MinPlacements: 0,
		MaxPlacements: 0,
	})
	b := ranje.NewRange(api.RangeID(5678), &ranje.ReplicationConfig{
		TargetActive:  0,
		MinActive:     0,
		MaxActive:     0,
		MinPlacements: 0,
		MaxPlacements: 0,
	})
	b.Children = []api.RangeID{a.Meta.Ident}

	// Act
	err = systemUnderTest.PutRanges([]*ranje.Range{a, b})
	if err != nil {
		t.Error(err)
		return
	}

	got, err := systemUnderTest.GetChildren(b.Meta.Ident)
	if err != nil {
		t.Error(err)
		return
	}

	// Assert
	want := []api.RangeID{a.Meta.Ident}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetChildren() mismatch (-want +got):\n%s", diff)
	}
}

func TestPutSomethingGetPlacements(t *testing.T) {
	// Arrange
	db := freshTestDB()
	defer db.Close()
	systemUnderTest, err := persisterSQL.New(db)
	if err != nil {
		t.Error(err)
		return
	}
	a := ranje.NewRange(api.RangeID(1234), &ranje.ReplicationConfig{
		TargetActive:  0,
		MinActive:     0,
		MaxActive:     0,
		MinPlacements: 0,
		MaxPlacements: 0,
	})
	a.Placements = []*ranje.Placement{
		{
			NodeID:       "SomeNodeID A",
			StateCurrent: api.PsMissing,
			StateDesired: api.PsInactive,
		},
	}
	err = systemUnderTest.PutRanges([]*ranje.Range{a})
	if err != nil {
		t.Error(err)
		return
	}

	// Act
	got, err := systemUnderTest.GetPlacements(a.Meta.Ident)
	if err != nil {
		t.Error(err)
		return
	}

	// Assert
	want := a.Placements
	opts := []cmp.Option{
		cmpopts.IgnoreUnexported(ranje.Placement{}),
		cmpopts.IgnoreFields(ranje.Placement{}, "Mutex"),
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("GetPlacements() mismatch (-want +got):\n%s", diff)
	}
}

func TestPutSomethingGetSomething(t *testing.T) {
	// Arrange
	db := freshTestDB()
	defer db.Close()
	systemUnderTest, err := persisterSQL.New(db)
	if err != nil {
		t.Error(err)
		return
	}

	// Act
	err = systemUnderTest.PutRanges([]*ranje.Range{
		ranje.NewRange(api.RangeID(1234), &ranje.ReplicationConfig{
			TargetActive:  0,
			MinActive:     0,
			MaxActive:     0,
			MinPlacements: 0,
			MaxPlacements: 0,
		}),
	})
	if err != nil {
		t.Error(err)
		return
	}

	got, err := systemUnderTest.GetRanges()
	if err != nil {
		t.Error(err)
		return
	}

	// Assert
	want := []*ranje.Range{
		ranje.NewRange(api.RangeID(1234), &ranje.ReplicationConfig{
			TargetActive:  0,
			MinActive:     0,
			MaxActive:     0,
			MinPlacements: 0,
			MaxPlacements: 0,
		}),
	}
	opts := []cmp.Option{
		cmpopts.IgnoreUnexported(ranje.Range{}),
		cmpopts.IgnoreFields(ranje.Range{}, "Mutex"),
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("GetRanges() mismatch (-want +got):\n%s", diff)
	}
}
