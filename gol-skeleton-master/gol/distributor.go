package gol

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
	"uk.ac.bris.cs/gameoflife/util"
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
	defer close(c.events)

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
	//connect to the engine via RPC
	client, err := rpc.Dial("tcp", p.EngineAddress)
	if err != nil {
		panic(fmt.Errorf("connect to Game of Life engine: %w", err))
	}
	defer client.Close()

	//start the evolution in the engine
	var startReply StartReply
	if err := client.Call("GolEngine.StartEvolution", EvolveRequest{
		World:   world,
		Turns:   p.Turns,
		Threads: p.Threads,
	}, &startReply); err != nil {
		panic(fmt.Errorf("start evolution via RPC: %w", err))
	}
	//help to implement the 's' and 'p'
	fetchSnapshot := func() Snapshot {
		var snapshot Snapshot
		if err := client.Call("GolEngine.GetSnapshot", Empty{}, &snapshot); err != nil {
			panic(fmt.Errorf("get engine snapshot: %w", err))
		}
		return snapshot
	}

	state := fetchSnapshot()
	world = state.World
	turn := state.CompletedTurns
	paused := state.Paused

	for y := range world {
		for x, cell := range world[y] {
			if cell == 255 {
				c.events <- CellFlipped{
					CompletedTurns: turn,
					Cell:           util.Cell{X: x, Y: y},
				}
			}
		}
	}
	c.events <- TurnComplete{CompletedTurns: turn}

	currentState := Executing
	if paused {
		currentState = Paused
	}
	c.events <- StateChange{turn, currentState}

	aliveTicker := time.NewTicker(2 * time.Second)
	defer aliveTicker.Stop()

	applySnapshot := func(snapshot Snapshot) {
		flipped := make([]util.Cell, 0)
		for y := range world {
			for x := range world[y] {
				if world[y][x] != snapshot.World[y][x] {
					flipped = append(flipped, util.Cell{X: x, Y: y})
				}
			}
		}
		if len(flipped) > 0 {
			c.events <- CellsFlipped{
				CompletedTurns: snapshot.CompletedTurns,
				Cells:          flipped,
			}
		}
		if snapshot.CompletedTurns != turn {
			c.events <- TurnComplete{CompletedTurns: snapshot.CompletedTurns}
			turn = snapshot.CompletedTurns
		}
		world = snapshot.World
	}

	finish := func(snapshot Snapshot) {
		applySnapshot(snapshot)
		aliveCells := make([]util.Cell, 0)
		for y := range world {
			for x, cell := range world[y] {
				if cell == 255 {
					aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
				}
			}
		}

		outputWorld(c, world, turn)
		c.events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}
		c.events <- StateChange{turn, Quitting}
	}

	for {
		select {
		case <-aliveTicker.C:
			var countReply AliveCountReply
			err := client.Call("GolEngine.GetAliveCells", AliveCountRequest{}, &countReply)
			if err != nil {
				panic(fmt.Errorf("get alive-cell count: %w", err))
			}
			c.events <- AliveCellsCount{
				CompletedTurns: countReply.CompletedTurns,
				CellsCount:     countReply.CellsCount,
			}
			state = Snapshot{
				World:          countReply.World,
				CompletedTurns: countReply.CompletedTurns,
				Paused:         countReply.Paused,
				Finished:       countReply.Finished,
			}
			applySnapshot(state)

			if state.Paused != paused {
				paused = state.Paused
				newState := Executing
				if paused {
					newState = Paused
				}
				c.events <- StateChange{turn, newState}
			}

			if state.Finished {
				finish(state)
				return
			}

		case key, ok := <-c.keyPress:
			if !ok {
				c.keyPress = nil
				continue
			}

			switch key {
			case 'p':
				method := "GolEngine.Pause"
				if paused {
					method = "GolEngine.Resume"
				}
				var snapshot Snapshot
				if err := client.Call(method, Empty{}, &snapshot); err != nil {
					panic(fmt.Errorf("change engine pause state: %w", err))
				}
				applySnapshot(snapshot)
				paused = snapshot.Paused
				newState := Executing
				if paused {
					newState = Paused
					log.Printf("Paused at turn %d", turn)
				} else {
					log.Print("Continuing")
				}
				c.events <- StateChange{turn, newState}

			case 's':
				snapshot := fetchSnapshot()
				outputWorld(c, snapshot.World, snapshot.CompletedTurns)

			case 'q':
				snapshot := fetchSnapshot()
				outputWorld(c, snapshot.World, snapshot.CompletedTurns)
				c.events <- StateChange{snapshot.CompletedTurns, Quitting}
				return

			case 'k':
				var shutdownReply Empty
				if err := client.Call("GolEngine.Shutdown", Empty{}, &shutdownReply); err != nil {
					panic(fmt.Errorf("shut down Game of Life engine: %w", err))
				}
				finish(fetchSnapshot())
				return
			}
		}
	}
}

func outputWorld(c distributorChannels, world [][]uint8, turn int) {
	height := len(world)
	width := len(world[0])
	filename := fmt.Sprintf("%dx%dx%d", width, height, turn)
	c.ioCommand <- ioOutput
	c.ioFilename <- filename
	for y := range world {
		for x := range world[y] {
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
