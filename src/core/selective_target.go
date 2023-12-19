package core

type selectiveTargetImpl struct {
	Target
	selector func(metadata map[string]any) bool
}

func TargetWithSelect(target Target, selector func(metadata map[string]any) bool) SelectiveTarget {
	return &selectiveTargetImpl{
		Target:   target,
		selector: selector,
	}
}

func TargetWithoutSelect(target Target) SelectiveTarget {
	return &selectiveTargetImpl{
		Target: target,
		selector: func(metadata map[string]any) bool {
			return true
		},
	}
}

func (t *selectiveTargetImpl) Select(metadata map[string]any) bool {
	return t.selector(metadata)
}
