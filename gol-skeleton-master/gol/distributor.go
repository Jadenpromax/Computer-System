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

	var initialKey rune
	hasInitialKey := false
	select {
	case key, ok := <-c.keyPress:
		if ok {
			initialKey = key
			hasInitialKey = true
		}
	default:
	}

	// runResponse carries the blocking RunGame RPC result back to the distributor loop,
	// so it can keep handling key presses and progress events while the call is in flight.
	type runResponse struct {
		result GameResult
		err    error
	}
	runDone := make(chan runResponse, 1)
	request := EvolveRequest{
		World:       cloneWorld(world),
		Turns:       p.Turns,
		Threads:     p.Threads,
		GameID:      fmt.Sprintf("%d", time.Now().UnixNano()),
		StartPaused: hasInitialKey && initialKey == 'p',
	}

	// RunGame blocks until all turns finish, so call it concurrently while the
	// distributor continues handling key presses and progress events.
	go func() {
		var result GameResult
		err := client.Call("GolEngine.RunGame", request, &result)
		runDone <- runResponse{result: result, err: err}
	}()

	// Wait until the blocking RunGame RPC has initialized the broker state.
	fetchSnapshot := func() Snapshot {
		var snapshot Snapshot
		if err := client.Call("GolEngine.GetSnapshot", Empty{}, &snapshot); err != nil {
			panic(fmt.Errorf("get engine snapshot: %w", err))
		}
		if snapshot.Error != "" {
			panic(fmt.Errorf("engine computation failed: %s", snapshot.Error))
		}
		return snapshot
	}

	var completion *runResponse
	var state Snapshot
	startupTicker := time.NewTicker(time.Millisecond)

	// Wait until the broker initializes the world. If RunGame finishes first, use
	// its final result; otherwise, fetch snapshots until the world is available.
	for state.GameID != request.GameID {
		select {
		case result := <-runDone:
			if result.err != nil {
				panic(fmt.Errorf("run game via RPC: %w", result.err))
			}
			completion = &result
			state = Snapshot{
				World:          result.result.World,
				CompletedTurns: result.result.CompletedTurns,
				Finished:       true,
				GameID:         request.GameID,
			}
		case <-startupTicker.C:
			state = fetchSnapshot()
		}
	}
	startupTicker.Stop()

	// Report the input board as turn 0; the broker may already have advanced
	// by the time its first snapshot reaches the controller.
	world = cloneWorld(request.World)
	turn := 0
	paused := request.StartPaused

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
	c.events <- TurnComplete{CompletedTurns: 0}
	c.events <- StateChange{CompletedTurns: 0, NewState: Executing}
	if paused {
		c.events <- StateChange{CompletedTurns: 0, NewState: Paused}
	}

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

	if completion != nil {
		finish(state)
		return
	}

	var queuedKeys <-chan rune
	if hasInitialKey && !request.StartPaused {
		initialKeys := make(chan rune, 1)
		initialKeys <- initialKey
		queuedKeys = initialKeys
	}

	handleKey := func(key rune) bool {
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
			if snapshot.Error != "" {
				panic(fmt.Errorf("engine computation failed: %s", snapshot.Error))
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
			return true

		case 'k':
			var shutdownReply Empty
			if err := client.Call("GolEngine.Shutdown", Empty{}, &shutdownReply); err != nil {
				panic(fmt.Errorf("shut down Game of Life engine: %w", err))
			}
			result := <-runDone
			if result.err != nil {
				panic(fmt.Errorf("run game via RPC: %w", result.err))
			}
			finish(Snapshot{
				World:          result.result.World,
				CompletedTurns: result.result.CompletedTurns,
				Finished:       true,
			})
			return true
		}
		return false
	}

	//keep tracking runDown, aliveTicker and keyPress
	for {
		select {
		case result := <-runDone:
			if result.err != nil {
				panic(fmt.Errorf("run game via RPC: %w", result.err))
			}
			finish(Snapshot{
				World:          result.result.World,
				CompletedTurns: result.result.CompletedTurns,
				Finished:       true,
			})
			return

		case <-aliveTicker.C:
			var countReply AliveCountReply
			err := client.Call("GolEngine.GetAliveCells", AliveCountRequest{}, &countReply)
			if err != nil {
				panic(fmt.Errorf("get alive-cell count: %w", err))
			}
			if countReply.Error != "" {
				panic(fmt.Errorf("engine computation failed: %s", countReply.Error))
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
				GameID:         countReply.GameID,
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

		case key := <-queuedKeys:
			queuedKeys = nil
			if handleKey(key) {
				return
			}

		case key, ok := <-c.keyPress:
			if !ok {
				c.keyPress = nil
				continue
			}
			if handleKey(key) {
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
