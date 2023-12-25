package core

import (
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

func (p *Pipeline) Run() (done chan struct{}, err error) {
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

func (p *Pipeline) writeWork(outCh <-chan any) (done chan struct{}) {
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
			go func(w any) {
				defer func() {
					p.logPipeline("Finish writing work to target")
					workers.Done()
				}()
				p.target.Write(w)
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

func (p *Pipeline) runPipeline(sourceChan <-chan any) <-chan any {
	p.logPipeline("Start launching pipeline")
	for actionId, action := range p.actions {
		outCh := make(chan any)
		stage := pipelineStage{
			pipelineName: p.name,
			actionId:     actionId,
			action:       action,
			input:        sourceChan,
			output:       outCh,
		}
		go stage.run()
		sourceChan = outCh
	}
	p.logPipeline("Finish launching pipeline")
	return sourceChan
}

func (p *Pipeline) logPipeline(msg string) {
	log.Info().Str("pipeline", p.name).Msg(msg)
}

func (p *Pipeline) logWork(work any, msg string) {
	log.Debug().Str("pipeline", p.name).Msg(msg)
}

func (p *Pipeline) logError(err error, msg string) {
	log.Error().Err(err).Msg(msg)
}

type pipelineStage struct {
	pipelineName string
	actionId     int
	action       Action
	input        <-chan any
	output       chan<- any
}

func (s *pipelineStage) run() {
	wg := new(sync.WaitGroup)
	for work := range s.input {
		if work == nil {
			s.logAction("Input channel produce nil work")
			break
		}
		wg.Add(1)
		go s.runWorker(wg, work)
	}
	wg.Wait()
	s.logAction("Waiting action workers")
	wg.Wait()
	s.logAction("Invoking action.Done()")
	s.action.Done(s.output)
	s.logAction("Finish action.Done(), producing nil")
	s.output <- nil
}

func (s *pipelineStage) runWorker(wg *sync.WaitGroup, work any) {
	defer func() {
		s.logAction("Worker is done")
		wg.Done()
	}()
	s.action.Do(work, s.output)
}

func (s *pipelineStage) logAction(msg string) {
	log.Info().Str("pipeline", s.pipelineName).Int("action", s.actionId).Msg(msg)
}
