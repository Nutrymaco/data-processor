package core

type Work struct {
	Data     []byte
	Object   any
	Metadata map[string]string
}

func NewStringWork(data string) *Work {
	return NewWork([]byte(data))
}

func NewWork(data []byte) *Work {
	return &Work{
		Data:     data,
		Metadata: map[string]string{},
	}
}

func (w *Work) ReadString() string {
	return string(w.Data)
}

func (w *Work) WithMetadata(metadata map[string]string) *Work {
	w.Metadata = metadata
	return w
}
