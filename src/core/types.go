package core

type Source interface {
	Read() (<-chan any, error)
}

type sourceImpl[I any] struct {
	read func(f func(I)) error
}

func NewSource[I any](read func(f func(I)) error) Source {
	return &sourceImpl[I]{
		read: read,
	}
}

func (s sourceImpl[I]) Read() (<-chan any, error) {
	ch := make(chan any)
	s.read(func(i I) {
		ch <- i
	})
	return ch, nil
}

type Target interface {
	Write(work any)
	Done()
}

type Pipe interface {
	Target
	Source
}

type Action interface {
	Do(work any, out chan<- any)
	Done(out chan<- any)
}

type NodesInfo struct {
	hosts map[string]string
}

type NodeIdManager interface {
	GetMyId() string
	GetMasterNodeId() string
}
