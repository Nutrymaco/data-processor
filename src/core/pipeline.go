package core

import (
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
)

type Pipeline struct {
	name    string
	source  Source
	target  Target
	actions []Action
}

func NewPipeline(name string, source Source, target Target, actions []Action) *Pipeline {
	return &Pipeline{
		name:    name,
		source:  source,
		target:  target,
		actions: actions,
	}
}

func (p *Pipeline) Process() (done chan struct{}, err error) {
	p.logPipeline("Start reading source")
	sourceCh, err := p.source.Read()
	if err != nil {
		p.logError(err, "Error reading source")
		return nil, err
	}
	outCh := p.runPipeline(sourceCh)
	done = p.writeWork(outCh)
	return done, nil
}

func (p *Pipeline) writeWork(outCh chan *Work) (done chan struct{}) {
	p.logPipeline("Start writing to target")
	done = make(chan struct{})
	go func() {
		workers := new(sync.WaitGroup)
		for work := range outCh {
			if work == nil {
				break
			}
			p.logWork(work, "Start writing work to target")
			p.logPipeline("Start writing work to target")
			workers.Add(1)
			go func(w *Work) {
				defer func() {
					p.logPipeline("Finish writing work to target")
					workers.Done()
				}()
				err := p.target.Write(w)
				if err != nil {
					p.logError(err, "Error while writing work to target, panicking")
					panic(err)
				}
			}(work)
		}
		workers.Wait()
		p.logPipeline("Start target.Done()")
		p.target.Done()
		p.logPipeline("Finish target.Done()")
		p.logPipeline("Finish writing to target")
		done <- struct{}{}
	}()
	return done
}

func (p *Pipeline) runPipeline(sourceChan chan *Work) chan *Work {
	p.logPipeline("Start launching pipeline")
	for actionId, action := range p.actions {
		outCh := make(chan *Work)
		go func(in, out chan *Work, actionId int, action Action) {
			workers := new(sync.WaitGroup)
			for work := range in {
				if work == nil {
					p.logAction(actionId, "Input channel produce nil work")
					break
				}
				p.logAction(actionId, "Input channel produced work, start worker")
				workers.Add(1)
				go func(w *Work) {
					defer func() {
						p.logAction(actionId, "Worker is done")
						workers.Done()
					}()
					action.Do(w, out)
				}(work)
			}
			p.logAction(actionId, "Waiting action workers")
			workers.Wait()
			p.logAction(actionId, "Invoking action.Done()")
			action.Done(out)
			p.logAction(actionId, "Finish action.Done(), producing nil")
			out <- nil
		}(sourceChan, outCh, actionId, action)
		sourceChan = outCh
	}
	p.logPipeline("Finish launching pipeline")
	return sourceChan
}

func (p *Pipeline) logPipeline(msg string) {
	log.Info().Str("pipeline", p.name).Msg(msg)
}

func (p *Pipeline) logAction(actionId int, msg string) {
	log.Info().Str("pipeline", p.name).Int("action", actionId).Msg(msg)
}

func (p *Pipeline) logWork(work *Work, msg string) {
	log.Debug().Str("pipeline", p.name).Str("work.metadata", fmt.Sprint(work.Metadata)).Msg(msg)
}

func (p *Pipeline) logError(err error, msg string) {
	log.Error().Err(err).Msg(msg)
}
