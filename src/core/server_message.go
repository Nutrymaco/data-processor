package core

import (
	"bytes"
	"io"
	"sync/atomic"
)

const MagicNumber = byte(77)

type serverMessage struct {
	buf         bytes.Buffer
	msgSizeRead [2]bool
	msgSize     int
	complete    atomic.Bool
}

func NewServerMessage() *serverMessage {
	return &serverMessage{}
}

func (m *serverMessage) consume(reader io.Reader) error {
	if m.msgSize == -1 {
		msgSize := [2]byte{}
		n, err := reader.Read(msgSize[:])
		if n == 0 && err != nil {
			return err
		}
		if n == 1 && m.msgSizeRead[0] == false {
			m.msgSize = int(msgSize[0])
			m.msgSizeRead[0] = true
		} else if n == 1 && m.msgSizeRead[1] == false {
			m.msgSize = m.msgSize << 8
			m.msgSize += int(msgSize[0])
		}
	}
	m.buf.ReadFrom(reader)
	if m.buf.Len() == m.msgSize {
		m.complete.Store(true)
	}
	return nil
}

func (m *serverMessage) isComplete() bool {
	return m.complete.Load()
}

func (m *serverMessage) convertToWork() (*Work, error) {
	keysCount, err := m.buf.ReadByte()
	if err != nil {
		return nil, err
	}
	metadata := map[string]string{}
	for i := 0; i < int(keysCount); i++ {
		key, err := m.buf.ReadString('\n')
		if err != nil {
			return nil, err
		}
		val, err := m.buf.ReadString('\n')
		if err != nil {
			return nil, err
		}
		metadata[key] = val
	}
	data := m.buf.Bytes()
	return &Work{
		Data:     data,
		Metadata: metadata,
	}, nil
}
