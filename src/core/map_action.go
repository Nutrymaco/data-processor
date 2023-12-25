package core

import (
	"sync"
)

type mapAction[I, O any] struct {
	do func(work I) O
}

func NewMapAction[I, O any](do func(work I) O) Action {
	return &mapAction[I, O]{do}
}

func (a *mapAction[I, O]) Do(work any, out chan<- any) {
	w := work.(I)
	out <- a.do(w)
}
func (a *mapAction[I, O]) Done(chan<- any) {

}

type flatMapAction[I, O any] struct {
	do func(work I, out func(O))
}

func NewFlatMapAction[I, O any](do func(work I, out func(O))) *flatMapAction[I, O] {
	return &flatMapAction[I, O]{
		do: do,
	}
}

func (a *flatMapAction[I, O]) Do(work any, out chan<- any) {
	a.do(work.(I), func(o O) {
		out <- o
	})
}

func (a *flatMapAction[I, O]) Done(chan<- any) {

}

type reduceAction[I, O any] struct {
	reducer func(O, I) O
	lock    *sync.Mutex
	result  O
}

func NewReduceAction[I, O any](seed O, reducer func(O, I) O) Action {
	return &reduceAction[I, O]{
		result:  seed,
		reducer: reducer,
		lock:    new(sync.Mutex),
	}
}

func (a *reduceAction[I, O]) Do(work any, out chan<- any) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.result = a.reducer(a.result, work.(I))
}

func (a *reduceAction[I, O]) Done(out chan<- any) {
	out <- a.result
}
