package core

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
