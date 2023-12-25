package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Work struct {
	data     string
	metadata map[string]string
}

func NewStringWork(data string) *Work {
	return &Work{data: data}
}

func (w *Work) WithMetadata(m map[string]string) *Work {
	w.metadata = m
	return w
}

func Test_AggregateTarget(t *testing.T) {
	tests := []struct {
		name      string
		input     []*Work
		selectors []func(work *Work) bool

		results [][]*Work
	}{
		{
			name: "3 selectors, non-overlapping",
			input: []*Work{
				NewStringWork("1-1").WithMetadata(map[string]string{"type": "1"}),
				NewStringWork("1-2").WithMetadata(map[string]string{"type": "1"}),
				NewStringWork("1-3").WithMetadata(map[string]string{"type": "1"}),
				NewStringWork("2-1").WithMetadata(map[string]string{"type": "2"}),
				NewStringWork("2-2").WithMetadata(map[string]string{"type": "2"}),
				NewStringWork("3-1").WithMetadata(map[string]string{"type": "3"}),
				NewStringWork("3-2").WithMetadata(map[string]string{"type": "3"}),
			},
			selectors: []func(work *Work) bool{
				func(work *Work) bool {
					return work.metadata["type"] == "1"
				},
				func(work *Work) bool {
					return work.metadata["type"] == "2"
				},
				func(work *Work) bool {
					return work.metadata["type"] == "3"
				},
			},

			results: [][]*Work{
				{
					NewStringWork("1-1").WithMetadata(map[string]string{"type": "1"}),
					NewStringWork("1-2").WithMetadata(map[string]string{"type": "1"}),
					NewStringWork("1-3").WithMetadata(map[string]string{"type": "1"}),
				},
				{
					NewStringWork("2-1").WithMetadata(map[string]string{"type": "2"}),
					NewStringWork("2-2").WithMetadata(map[string]string{"type": "2"}),
				},
				{
					NewStringWork("3-1").WithMetadata(map[string]string{"type": "3"}),
					NewStringWork("3-2").WithMetadata(map[string]string{"type": "3"}),
				},
			},
		},
		{
			name: "2 selectors, second is always true",
			input: []*Work{
				NewStringWork("1-1").WithMetadata(map[string]string{"type": "1"}),
				NewStringWork("1-2").WithMetadata(map[string]string{"type": "1"}),
				NewStringWork("1-3").WithMetadata(map[string]string{"type": "1"}),
				NewStringWork("2-1").WithMetadata(map[string]string{"type": "2"}),
				NewStringWork("2-2").WithMetadata(map[string]string{"type": "123"}),
			},
			selectors: []func(work *Work) bool{
				func(work *Work) bool {
					return work.metadata["type"] == "1"
				},
				func(work *Work) bool {
					return true
				},
			},

			results: [][]*Work{
				{
					NewStringWork("1-1").WithMetadata(map[string]string{"type": "1"}),
					NewStringWork("1-2").WithMetadata(map[string]string{"type": "1"}),
					NewStringWork("1-3").WithMetadata(map[string]string{"type": "1"}),
				},
				{
					NewStringWork("2-1").WithMetadata(map[string]string{"type": "2"}),
					NewStringWork("2-2").WithMetadata(map[string]string{"type": "123"}),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			targets := []SelectiveTarget{}
			for _, sel := range tc.selectors {
				targets = append(targets, TargetWithSelect(
					NewArrayTarget[*Work](), sel,
				))
			}
			agg := NewAggregatedTarget(targets...)
			for _, w := range tc.input {
				agg.Write(w)
			}
			for i, target := range targets {
				expected := tc.results[i]
				actual := target.(*selectiveTargetImpl[*Work]).Target.(*ArrayTarget[*Work]).GetArray()
				assert.Equal(t, expected, actual, "target %d", i)
			}
		})
	}
}
