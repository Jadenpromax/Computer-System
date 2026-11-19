package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/rpc"
	"sync"
    "strings"
	"uk.ac.bris.cs/gameoflife/gol"
)

func main() {
	listenAddress := flag.String("listen", ":8030", "TCP address for the Game of Life engine")
    workers := flag.String(
        "workers",
        "",
        "Comma-sepearated worker addresses",
    ) 
	flag.Parse()

	engine := gol.NewEngine()
    if *workers != "" {
        if err := engine.SetWorkers(strings.Split(*workers, ",")); err != nil {
            log.Fatal(err)
        }
    }

    server := rpc.NewServer()
    if err := server.RegisterName("GolEngine", engine); err != nil {
        log.Fatal(err)
    } 


	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Gol engine listening on %s", *listenAddress)

	go func() {
		<-engine.StopSignal()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("close listener: %v", err)
		}
	}()

	var clients sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("accept connection: %v", err)
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
