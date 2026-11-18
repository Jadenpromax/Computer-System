package gol

import (
	"fmt"
	"sync"
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

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	//track the state before the turn starts
	paused := false
	handleKey :=func(key rune) bool{
		switch key{
		case 'p':
			paused = !paused
			state:= Executing
			if paused {
				state = Paused
			}
			c.events <- StateChange{turn,state}

		case 's':
			outputWorld(c,world,turn)
			
		case 'q':
			outputWorld(c,world,turn)
			c.events <- StateChange{turn, Quitting}
			close(c.events)
			return true
		}
		return false
	}


	// Execute all turns of the Game of Life.
	//try to implement a parallel version
	// 根据params中的线程决定多少个worker
	//create a number of workers based on the number of threads specified in params.threads
	workers :=p.Threads
	if workers < 1 {
		workers = 1
	}
	if workers > p.ImageHeight {
		workers = p.ImageHeight
	}
    //each worker will be respnsible for a portion of the image, and will calculate the next state of the cells in that portion.
	for turn < p.Turns {
		//check if paused
		if paused{
			if handleKey(<-c.keyPress){
				return
			}
			continue
		}

		select{
		case key := <-c.keyPress:
			if handleKey(key){
				return
			}
			continue
		default:
		}

		nextWorld := make([][]uint8, p.ImageHeight)

		for y:= range nextWorld {
			nextWorld[y] = make([]uint8, p.ImageWidth)
		}
		var wg sync.WaitGroup

		for worker := 0; worker < workers; worker++ {
			startY := worker * p.ImageHeight / workers
			endY := (worker + 1) * p.ImageHeight / workers
			//splite↑
			wg.Add(1)
			go func(startY,endY int) {
				defer wg.Done()
				for y := startY; y < endY; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						aliveNeighbours:=0
						for dy := -1; dy <= 1; dy++ {
							for dx := -1; dx <= 1; dx++{
								if dx ==0 && dy == 0{
									continue
								}
								nx := (x + dx + p.ImageWidth) % p.ImageWidth
								ny := (y + dy + p.ImageHeight) % p.ImageHeight
								if world[ny][nx] == 255 {
									aliveNeighbours++
								}
							}
						}
						
						if world[y][x] == 255 {
							if aliveNeighbours == 2 || aliveNeighbours == 3{
								nextWorld[y][x]=255
							}
						
						}	else if aliveNeighbours == 3 {
						nextWorld[y][x]=255
						}
					}
				}
			}(startY,endY)
		}
		wg.Wait()

		completedTurns := turn + 1

		//compare the current world and the next world to find which cells have changed state,and send a CellsFlipped
		//event to the GUI for each cell that has changed state
		flipped := make([]util.Cell, 0)
		for y := range world{
			for x:= range world[y]{
				if world[y][x] != nextWorld[y][x]{
					flipped = append(flipped, util.Cell{X:x,Y:y})
				}
			}

		}


		world = nextWorld
		turn= completedTurns


		if len(flipped)>0{
			c.events <- CellsFlipped{
				CompletedTurns: turn,
				Cells: flipped,
			}
		}

		c.events <- TurnComplete{
			CompletedTurns: turn,
		}

		select {
		case <- ticker.C:
			count := countAliveNeighbours(world)
			c.events <- AliveCellsCount{
				CompletedTurns: turn,
				CellsCount: count,
			}
		default:
		}
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

	//old version of output world specific to step 4↓
	// filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	// c.ioCommand <- ioOutput
	// c.ioFilename <- filename
	// for y:=0 ; y < p.ImageHeight; y++ {
	// 	for x := 0 ; x < p.ImageWidth; x++ {
	// 		c.ioOutput <- world[y][x]
	// 	}
	// }

	// c.ioCommand <- ioCheckIdle
	// <-c.ioIdle

	// c.events <- ImageOutputComplete{
	// 	CompletedTurns: turn,
	// 	Filename:       filename,
	// }
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




func countAliveNeighbours(world [][]uint8) int{
	count := 0
	for y :=range world {
		for x := range world[y]{
			if world[y][x] == 255{
				count++
			}
		}
	}
	return count
}
