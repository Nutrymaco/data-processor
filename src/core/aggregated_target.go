package core

import "fmt"

type SelectiveTarget interface {
	Target
	Select(metadata map[string]string) bool
}

type aggregatedTarget struct {
	targets []SelectiveTarget
}

func NewAggregatedTarget(targets ...SelectiveTarget) *aggregatedTarget {
	return &aggregatedTarget{
		targets: targets,
	}
}

func (t *aggregatedTarget) Write(work *Work) {
	fmt.Println("[agg target] search target to write work")
	for _, target := range t.targets {
		if target.Select(work.Metadata) {
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

type selectiveTargetImpl struct {
	Target
	selector func(metadata map[string]string) bool
}

func TargetWithSelect(target Target, selector func(metadata map[string]string) bool) SelectiveTarget {
	return &selectiveTargetImpl{
		Target:   target,
		selector: selector,
	}
}

func (t *selectiveTargetImpl) Select(metadata map[string]string) bool {
	return t.selector(metadata)
}
