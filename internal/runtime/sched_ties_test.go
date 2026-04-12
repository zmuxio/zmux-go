package runtime

import "testing"

func TestSnapshotBatchTiePrefsCopiesPreferredStreamHeads(t *testing.T) {
	t.Parallel()

	groupKey := GroupKey{Kind: 1, Value: 7}
	state := &BatchState{
		HasPreferredGroupHead: true,
		PreferredGroupHead:    groupKey,
		PreferredStreamHead: map[GroupKey]uint64{
			groupKey: 4,
		},
	}

	prefs := snapshotBatchTiePrefs(state)
	state.PreferredStreamHead[groupKey] = 8

	if !prefs.hasGroup || prefs.group != groupKey {
		t.Fatalf("snapshot group prefs = (%v, %+v), want (true, %+v)", prefs.hasGroup, prefs.group, groupKey)
	}
	if got := prefs.streams[groupKey]; got != 4 {
		t.Fatalf("snapshot stream pref = %d, want 4", got)
	}
}
