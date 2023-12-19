package core

import (
	"fmt"
	"sync/atomic"
)

type Processor struct {
	sources []Source
	targets []SelectiveTarget
	actions []Action
}

func NewProcessor(sources []Source, targets []SelectiveTarget, actions []Action) *Processor {
	return &Processor{
		sources: sources,
		targets: targets,
		actions: actions,
	}
}

func (p *Processor) Process() error {
	sourceChannels := []chan *Work{}
	for _, source := range p.sources {
		workCh, err := source.Read()
		if err != nil {
			return err
		}
		sourceChannels = append(sourceChannels, workCh)
	}
	sourceCh := p.combineChannels(sourceChannels)
	outCh := p.runPipeline(sourceCh)
	p.writeToTargets(outCh)
	return nil
}

func (p *Processor) writeToTargets(outCh chan *Work) {
	workersCounter := new(atomic.Int32)
	for work := range outCh {
		if work == nil {
			for _, target := range p.targets {
				target.Done()
			}
			break
		}
		for _, target := range p.targets {
			if target.Select(work.Metadata) {
				workersCounter.Add(1)
				go func(w *Work) {
					target.Write(w)
					workersCounter.Add(-1)
				}(work)
				break
			}
		}
	}
	for workersCounter.Load() != 0 {

	}
}

func (p *Processor) combineChannels(channels []chan *Work) chan *Work {
	combined := make(chan *Work)
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
				fmt.Println("sent done from sources")
				combined <- nil
				break
			}
		}
	}()
	return combined
}

func (p *Processor) runPipeline(sourceChan chan *Work) chan *Work {
	for _, action := range p.actions {
		outCh := make(chan *Work)
		workersCounter := new(atomic.Int32)
		go func(in, out chan *Work, action Action) {
			for work := range in {
				if work == nil {
					break
				}
				workersCounter.Add(1)
				go func(w *Work) {
					action.Do(w, out)
					workersCounter.Add(-1)
				}(work)
			}
			for workersCounter.Load() != 0 {

			}
			action.Done(out)
			out <- nil
		}(sourceChan, outCh, action)
		sourceChan = outCh
	}
	return sourceChan
}
