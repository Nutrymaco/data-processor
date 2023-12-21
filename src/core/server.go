package core

import (
	"fmt"
	"net"
	"sync"
)

type Server struct {
	listener    net.Listener
	inConns     []net.Conn
	inConnsBuf  []*serverMessage
	outConns    map[string]net.Conn
	workersLock sync.Mutex
	workers     []ServerWorker
	msgBuffer   []*Message
}

type Message struct {
	work *Work
}

func (s *Server) Run(port int, nodes map[string]string) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		panic(err)
	}
	s.listener = lis
	go s.dispatch()
	go s.consumeConnections()
	go s.createConnections(nodes)
}

func (s *Server) consumeConnections() {
	for {
		inConn, err := s.listener.Accept()
		if err != nil {
			panic(err)
		}
		s.inConns = append(s.inConns, inConn)
	}
}

func (s *Server) createConnections(nodes map[string]string) {
	for id, host := range nodes {
		outConn, err := net.Dial("tcp", host)
		if err != nil {
			panic(err)
		}
		s.outConns[id] = outConn
	}
}

func (s *Server) dispatch() {
	for {
		for _, msg := range s.msgBuffer {
			for _, worker := range s.workers {
				worker(msg)
			}
		}
	}
}

func (s *Server) readConns() {
	s.inConnsBuf = make([]*serverMessage, len(s.inConns))
	for {
		for i, conn := range s.inConns {
			msg := s.inConnsBuf[i]
			if msg == nil {
				msg = NewServerMessage()
				s.inConnsBuf[i] = msg
			}
			err := msg.consume(conn)
			if err != nil {
				panic(err)
			}
			if msg.isComplete() {
				work, err := msg.convertToWork()
				if err != nil {
					panic(err)
				}
				s.msgBuffer = append(s.msgBuffer, &Message{
					work: work,
				})
				s.inConnsBuf[i] = nil
			}
		}
	}
}

func (s *Server) registerWorker(worker *ServerWorker) {
	s.workersLock.Lock()
	s.workers = append(s.workers, *worker)
	s.workersLock.Unlock()
}
