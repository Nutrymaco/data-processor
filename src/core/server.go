package core

import (
	"net"
	"sync"
)

type Server struct {
	listener net.Listener
	conns       map[int]net.Conn
	workersLock sync.Mutex
	workers     []ServerWorker
	msgBuffer   []*Message
}

type Message struct {
}

func (s *Server) Run(port, ports []int) {
	go s.dispatch()
	go s.establishConnections(ports)
}

func (s *Server) establishConnections(ports []int) {
	for {
		for _, port := range ports {
			go func(port int) {
				conn, err := 
			}(port)
		}
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

func readMessage() {

}

func (s *Server) registerWorker(worker *ServerWorker) {
	s.workersLock.Lock()
	s.workers = append(s.workers, *worker)
	s.workersLock.Unlock()
}
