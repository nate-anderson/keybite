package store_test

import (
	"keybite/store"
	"testing"
)

func TestArraySelector(t *testing.T) {
	testIDs := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 200}
	selector := store.NewArraySelector(testIDs)
	for _, testID := range testIDs {
		selector.Next()
		if selector.Select() != testID {
			t.Logf("selector value %d != testID %d", selector.Select(), testID)
			t.Fail()
		}
	}
}

func TestRangeSelector(t *testing.T) {
	var testMin uint64 = 3
	var testMax uint64 = 65
	selector := store.NewRangeSelector(testMin, testMax)
	for i := testMin; i <= testMax; i++ {
		selector.Next()
		if i != selector.Select() {
			t.Logf("selector value %d != iterator %d", selector.Select(), i)
			t.Fail()
		}
	}
}

func TestSingleSelector(t *testing.T) {
	testSelection := uint64(120)
	selector := store.NewSingleSelector(testSelection)
	selector.Next()
	if selector.Select() != testSelection {
		t.Logf("selector value %d != test value %d", selector.Select(), testSelection)
		t.Fail()
	}
}
