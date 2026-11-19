package main
import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/gol"
)


func main(){
    listenAddress := flag.String("listen", ":8030", "TCP address for the Game of Life engine")
    flag.Parse()

    server := rpc.NewServer()
    if err := server.RegisterName("GolEngine", new(gol.Engine)); err != nil {
        log.Fatal(err)
    }

    listener, err := net.Listen("tcp", *listenAddress)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Gol engine listening on %s", *listenAddress)

    server.Accept(listener)

}

