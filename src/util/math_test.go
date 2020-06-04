package util_test

import (
	"keybite/util"
	"testing"
)

func TestMin(t *testing.T) {
	haves := [][]int64{
		[]int64{1, 2},
		[]int64{2, 10},
		[]int64{30, -30},
		[]int64{0, 0},
	}

	wants := []int64{
		1,
		2,
		-30,
		0,
	}

	for i, have := range haves {
		util.Equals(t, wants[i], util.Min(have[0], have[1]))
	}
}

func TestMax(t *testing.T) {
	haves := [][]int64{
		[]int64{1, 2},
		[]int64{2, 10},
		[]int64{30, -30},
		[]int64{0, 0},
	}

	wants := []int64{
		2,
		10,
		30,
		0,
	}

	for i, have := range haves {
		util.Equals(t, wants[i], util.Max(have[0], have[1]))
	}
}
