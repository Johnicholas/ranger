// Code generated by "stringer -type=PlacementState -output=zzz_state_placement_string.go"; DO NOT EDIT.

package ranje

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[PsUnknown-0]
	_ = x[PsPending-1]
	_ = x[PsLoading-2]
	_ = x[PsDropped-3]
}

const _PlacementState_name = "PsUnknownPsPendingPsLoadingPsDropped"

var _PlacementState_index = [...]uint8{0, 9, 18, 27, 36}

func (i PlacementState) String() string {
	if i >= PlacementState(len(_PlacementState_index)-1) {
		return "PlacementState(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _PlacementState_name[_PlacementState_index[i]:_PlacementState_index[i+1]]
}
