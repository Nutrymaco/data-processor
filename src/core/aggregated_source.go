package core

import (
	"sync/atomic"
)

type aggregatedSource struct {
	sources []Source
}

func NewAggregatedSource(sources ...Source) *aggregatedSource {
	return &aggregatedSource{
		sources: sources,
	}
}

func (s *aggregatedSource) Read() (<-chan any, error) {
	channels := []<-chan any{}
	for _, source := range s.sources {
		workCh, err := source.Read()
		if err != nil {
			return nil, err
		}
		channels = append(channels, workCh)
	}

	combined := make(chan any)
	doneCount := new(atomic.Int32)
	doneCount.Store(int32(len(channels)))
	go func() {
		for {
			for _, ch := range channels {
				select {
				case work := <-ch:
					if work == nil {
						doneCount.Add(-1)
					} else {
						combined <- work
					}
				}
			}
			if doneCount.Load() == 0 {
				combined <- nil
				break
			}
		}
	}()
	return combined, nil
}
