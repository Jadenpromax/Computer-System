package gol

import (
	"fmt"
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/util"
	"time"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
	keyPress   <-chan rune
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {
	world := make([][]uint8, p.ImageHeight)

	for y := 0; y < p.ImageHeight; y++ {
		world[y] = make([]uint8, p.ImageWidth)
	}

	c.ioCommand <- ioInput
	c.ioFilename <- fmt.Sprintf(
		"%dx%d",
		p.ImageWidth,
		p.ImageHeight,
	)
	//read the initial state of the world from the input channel
	//load in
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}



	turn := 0
	//send CellFlipped events for all cells that are alive when the image is loaded in
	for y := range world{
		for x,cell :=range world[y]{
			if cell == 255{
				c.events <-CellFlipped{
					CompletedTurns: turn,
					Cell: util.Cell{X:x,Y:y},
				}
			}
		}
	}


	c.events <- StateChange{turn, Executing}
	
	client, err := rpc.Dial("tcp", p.EngineAddress)
	if err != nil {
		panic(fmt.Errorf("connect to Game of Life engine: %w", err))
	}
	defer client.Close()

	var reply EvolveReply
	evolveDone := make(chan error,1)
	go func() {
		evolveDone <- client.Call("GolEngine.Evolve", EvolveRequest{
			World:   world,
			Turns:   p.Turns,
			Threads: p.Threads,
		}, &reply)
	}()
	
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	evolving := true
	for evolving {
		select {
		case <-ticker.C:
			var countReply AliveCountReply
			err := client.Call("GolEngine.GetAliveCells", AliveCountRequest{}, &countReply)
			if err != nil {
				panic(fmt.Errorf("get alive-cell count: %w", err))
			}
			c.events<-AliveCellsCount{
				CompletedTurns: countReply.CompletedTurns,
				CellsCount:     countReply.CellsCount,
			}
		case err := <-evolveDone:
			if err !=nil {
				panic(fmt.Errorf("evolve world via RPC: %w", err))
			}
			evolving = false
		}
	}

	world = reply.World
	for _, result := range reply.Result {
		turn = result.CompletedTurns

		if len(result.Flipped) > 0 {
			c.events <- CellsFlipped{
				CompletedTurns: turn,
				Cells:          result.Flipped,
			}
		}
		c.events <- TurnComplete{CompletedTurns: turn}
	}



	// Report the final state using FinalTurnCompleteEvent.
	aliveCells := make([]util.Cell, 0)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
				aliveCells = append(aliveCells, util.Cell{
					X: x,
					Y: y,
				})
			}
		}
	}

	outputWorld(c,world,turn)


	c.events <- FinalTurnComplete{
		CompletedTurns: turn,
		Alive:          aliveCells,
	}
	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}
	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}




func outputWorld(c distributorChannels,world [][]uint8,turn int){
	height := len(world)
	width := len(world[0])
	filename := fmt.Sprintf("%dx%dx%d", width, height, turn)
	c.ioCommand <- ioOutput
	c.ioFilename <- filename
	for y := range world{
		for x := range world[y]{
			c.ioOutput <- world[y][x]
		}
	}
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{
		CompletedTurns: turn,
		Filename:       filename,
	}
}





