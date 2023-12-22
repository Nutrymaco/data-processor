package core

import (
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
				NewArraySource([]*Work{
					NewStringWork("source1-1"), NewStringWork("source1-2"), NewStringWork("source1-3"),
				}, 1),
				NewArraySource([]*Work{
					NewStringWork("source2-1"), NewStringWork("source2-2"), NewStringWork("source2-3"),
				}, 1),
				NewArraySource([]*Work{
					NewStringWork("source2-1"), NewStringWork("source2-2"), NewStringWork("source2-3"),
				}, 1),
			},
		},
		{
			name: "different speed",
			sources: []Source{
				NewArraySource([]*Work{
					NewStringWork("source1-1"), NewStringWork("source1-2"), NewStringWork("source1-3"),
				}, 2),
				NewArraySource([]*Work{
					NewStringWork("source2-1"), NewStringWork("source2-2"), NewStringWork("source2-3"),
				}, 1),
				NewArraySource([]*Work{
					NewStringWork("source2-1"), NewStringWork("source2-2"), NewStringWork("source2-3"),
				}, 3),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := NewAggregatedSource(tc.sources...)
			ch, err := source.Read()
			assert.Nil(t, err)

			res := []*Work{}
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
	source1.Write(NewStringWork("test"))
	source2.Write(NewStringWork("test"))
	source3.Write(NewStringWork("test"))

	source1.Write(nil)
	source2.Write(nil)

	lock.Lock()
	source3.Write(nil)
	allSendNil = true
	lock.Unlock()
}

func indexOf(data string, works []*Work) int {
	for i, work := range works {
		if work.ReadString() == data {
			return i
		}
	}
	panic(fmt.Sprintf("not found %s in %s", data, fmt.Sprint(works)))
}
