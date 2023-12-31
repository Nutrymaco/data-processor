package core

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_DifferentSpeed(t *testing.T) {
	tests := []struct {
		name    string
		sources []Source
	}{
		{
			name: "same speed",
			sources: []Source{
				NewArraySource([]any{
					"source1-1", "source1-2", "source1-3",
				}, 1),
				NewArraySource([]any{
					"source2-1", "source2-2", "source2-3",
				}, 1),
				NewArraySource([]any{
					"source2-1", "source2-2", "source2-3",
				}, 1),
			},
		},
		{
			name: "different speed",
			sources: []Source{
				NewArraySource([]any{
					"source1-1", "source1-2", "source1-3",
				}, 2),
				NewArraySource([]any{
					"source2-1", "source2-2", "source2-3",
				}, 1),
				NewArraySource([]any{
					"source2-1", "source2-2", "source2-3",
				}, 3),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := NewAggregatedSource(tc.sources...)
			ch, err := source.Read()
			assert.Nil(t, err)

			res := []any{}
			end := false
			for !end {
				select {
				case w := <-ch:
					res = append(res, w)
					if w == nil {
						end = true
					}
				}
			}
			assert.Equal(t, 10, len(res), "size of works should be 9, not %d", len(res))
			assert.Nil(t, res[9])
		})
	}
}

func Test_NilProducedOnlyAfterAllSendNil(t *testing.T) {
	source1 := NewChannelPipe()
	source2 := NewChannelPipe()
	source3 := NewChannelPipe()
	aggSource := NewAggregatedSource(source1, source2, source3)
	aggCh, err := aggSource.Read()
	assert.Nil(t, err)

	lock := new(sync.Mutex)
	allSendNil := false
	go func() {
		for {
			select {
			case w := <-aggCh:
				fmt.Println("consume", w)
				if w == nil {
					lock.Lock()
					assert.True(t, allSendNil)
					lock.Unlock()
				}
			}
		}
	}()
	source1.Write("test")
	source2.Write("test")
	source3.Write("test")

	source1.Write(nil)
	source2.Write(nil)

	lock.Lock()
	source3.Write(nil)
	allSendNil = true
	lock.Unlock()
}

func Test_ErrorInRead(t *testing.T) {
	source := NewArraySource([]any{}, 0)
	errorSource := NewGenericSource(func() (<-chan any, error) {
		return nil, errors.New("error in source")
	})

	aggSource := NewAggregatedSource(source, errorSource)
	ch, err := aggSource.Read()
	assert.NotNil(t, err)
	assert.Nil(t, ch)
}

func indexOf(data string, works []any) int {
	for i, work := range works {
		if work == data {
			return i
		}
	}
	panic(fmt.Sprintf("not found %s in %s", data, fmt.Sprint(works)))
}
