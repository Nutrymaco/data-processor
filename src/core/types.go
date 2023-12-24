package core

type Source interface {
	Read() (chan *Work, error)
}

type Target interface {
	Write(work *Work)
	Done()
}

type Pipe interface {
	Target
	Source
}

type Action interface {
	Do(work *Work, out chan *Work)
	Done(out chan *Work)
}

type NodesInfo struct {
	hosts map[string]string
}

type NodeIdManager interface {
	GetMyId() string
	GetMasterNodeId() string
}
