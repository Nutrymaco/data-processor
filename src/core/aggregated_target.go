package core

import "fmt"

type SelectiveTarget interface {
	Target
	Select(any) bool
}

type aggregatedTarget struct {
	targets []SelectiveTarget
}

func NewAggregatedTarget(targets ...SelectiveTarget) *aggregatedTarget {
	return &aggregatedTarget{
		targets: targets,
	}
}

func (t *aggregatedTarget) Write(work any) {
	fmt.Println("[agg target] search target to write work")
	for _, target := range t.targets {
		if target.Select(work) {
			fmt.Println("[agg target] found target to write work")
			target.Write(work)
			break
		}
	}
}

func (t *aggregatedTarget) Done() {
	for _, target := range t.targets {
		target.Done()
	}
}

type selectiveTargetImpl[T any] struct {
	Target
	selector func(T) bool
}

func TargetWithSelect[T any](target Target, selector func(T) bool) SelectiveTarget {
	return &selectiveTargetImpl[T]{
		Target:   target,
		selector: selector,
	}
}

func (t *selectiveTargetImpl[T]) Select(work any) bool {
	return t.selector(work.(T))
}
