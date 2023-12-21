package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

const MagicNumber = byte(77)

type serverMessage struct {
	buf         bytes.Buffer
	msgSizeRead [2]bool
	msgSize     int
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

	i := 0
	if !m.magicNumberRead {
		if newData[0] != MagicNumber {
			panic(errors.New(fmt.Sprintf("first byte of message is not %d", MagicNumber)))
		}
		i++
		m.magicNumberRead = true
	}
	if !m.metadataRead {

	}

}

func (m *serverMessage) isComplete() bool {

}

func (m *serverMessage) convertToWork() *Work {

}
