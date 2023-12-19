package core

type Work struct {
	Data     []byte
	Metadata map[string]any
}

func NewStringWork(data string) *Work {
	return NewWork([]byte(data))
}

func NewWork(data []byte) *Work {
	return &Work{
		Data:     data,
		Metadata: map[string]any{},
	}
}

func (w *Work) ReadString() string {
	return string(w.Data)
}

func (w *Work) WithMetadata(metadata map[string]any) *Work {
	w.Metadata = metadata
	return w
}
