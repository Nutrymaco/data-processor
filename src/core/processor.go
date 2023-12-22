package core

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Processor struct {
	source  Source
	target  Target
	actions []Action
}

func NewProcessor(source Source, target Target, actions []Action) *Processor {
	return &Processor{
		source:  source,
		target:  target,
		actions: actions,
	}
}

func (p *Processor) Process() (done chan struct{}, err error) {
	sourceCh, err := p.source.Read()
	if err != nil {
		fmt.Println("[processor][error]", err)
		return nil, err
	}
	fmt.Println("[processor] source is ready")
	outCh := p.runPipeline(sourceCh)
	fmt.Println("[processor] pipeline launched, out channel:", outCh)
	done = p.writeWork(outCh)
	return done, nil
}

func (p *Processor) writeWork(outCh chan *Work) (done chan struct{}) {
	done = make(chan struct{})
	go func() {
		wg := new(sync.WaitGroup)
		fmt.Println("[processor] start writing work to target")
		for work := range outCh {
			if work == nil {
				fmt.Println("[processor] start target.Done()", p.target)
				wg.Add(1)
				go func() {
					defer wg.Done()
					p.target.Done()
				}()
				break
			}
			fmt.Println("[processor] start write work" + fmt.Sprint(&work) + " to target")
			wg.Add(1)
			go func(w *Work) {
				defer wg.Done()
				err := p.target.Write(w)
				if err != nil {
					fmt.Println("[processor][error]", err)
					panic(err)
				}
			}(work)
		}
		wg.Wait()
		done <- struct{}{}
	}()
	return done
}

func (p *Processor) runPipeline(sourceChan chan *Work) chan *Work {
	for _, action := range p.actions {
		outCh := make(chan *Work)
		go func(in, out chan *Work, action Action) {
			workersCounter := new(atomic.Int32)
			for work := range in {
				if work == nil {
					fmt.Println("[pipeline] chan produce nil work")
					break
				}
				fmt.Println("[pipeline] chan produce work, start worker")
				workersCounter.Add(1)
				go func(w *Work) {
					action.Do(w, out)
					fmt.Println("[pipeline] action is done")
					workersCounter.Add(-1)
				}(work)
			}
			for workersCounter.Load() != 0 {
			}
			action.Done(out)
			fmt.Println("[pipeline] action produce nil work", out)
			out <- nil
		}(sourceChan, outCh, action)
		sourceChan = outCh
	}
	return sourceChan
}
