package api

import (
	"errors"
)

var ErrNotFound = errors.New("not found")

type Node interface {

	// GetLoadInfo returns the LoadInfo for the given range.
	// Implementations should return NotFound if (from their point of view) the
	// range doesn't exist. This can happen when GetLoadInfo and Prepare and/or
	// DropRange are racing.
	GetLoadInfo(rID RangeID) (LoadInfo, error)

	// Prepare.
	Prepare(m Meta, p []Parent) error

	// AddRange
	AddRange(rID RangeID) error

	// PrepareDropRange
	PrepareDropRange(rID RangeID) error

	// DropRange
	// Range state will be set to NsDropping before calling this. If an error is
	// returned, the range will be forgotten. If no error is returned, the range
	// state will be set to NsDroppingError.
	DropRange(rID RangeID) error
}
