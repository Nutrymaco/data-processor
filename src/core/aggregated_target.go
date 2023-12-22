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

func (t *aggregatedTarget) Write(work *Work) error {
	fmt.Println("[agg target] search target to write work")
	for _, target := range t.targets {
		if target.Select(work.Metadata) {
			fmt.Println("[agg target] found target to write work")
			err := target.Write(work)
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
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

func TargetWithoutSelect(target Target) SelectiveTarget {
	return &selectiveTargetImpl{
		Target: target,
		selector: func(metadata map[string]string) bool {
			return true
		},
	}
}

func (t *selectiveTargetImpl) Select(metadata map[string]string) bool {
	return t.selector(metadata)
}
