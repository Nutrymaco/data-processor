package core

import (
	"fmt"
	"net"
	"sync"
)

type Server struct {
	listener    net.Listener
	inConns     []net.Conn
	inConnsBuf  [][]byte
	outConns    map[string]net.Conn
	workersLock sync.Mutex
	workers     []ServerWorker
	msgBuffer   []*Message
}

type Message struct {
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
	s.inConnsBuf = make([][]byte, len(s.inConns))
	for {
		for _, conn := range s.inConns {
			data := []byte{}
			_, err := conn.Read(data)
			for _, da
		}
	}
}

func haveEndSymbol(data []byte) int {
	for i := 0; i < len(data); i++ {
		
	}
}

func (s *Server) registerWorker(worker *ServerWorker) {
	s.workersLock.Lock()
	s.workers = append(s.workers, *worker)
	s.workersLock.Unlock()
}
