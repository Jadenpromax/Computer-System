package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/rpc"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
)

func main() {
	listenAddress := flag.String("listen", ":8031", "TCP address for the worker")
	flag.Parse()

	worker := gol.NewWorker()
	server := rpc.NewServer()
	if err := server.RegisterName("GolWorker", worker); err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Gol worker listening on %s", *listenAddress)

	go func() {
		<-worker.StopSignal()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("close worker listener: %v", err)
		}
	}()

	var clients sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("accept worker connection: %v", err)
			}
			break
		}

		clients.Add(1)
		go func() {
			defer clients.Done()
			server.ServeConn(conn)
		}()
	}
	clients.Wait()
}
