package gol

import (
	"fmt"
	"sync"
	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
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

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	turn := 0
	c.events <- StateChange{turn, Executing}

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
		nextWorld := make([][]uint8, p.ImageHeight)

		for y:= range nextWorld {
			nextWorld[y] = make([]uint8, p.ImageWidth)
		}
		var wg sync.WaitGroup

		for worker := 0; worker < workers; worker++ {
			startY := worker * p.ImageHeight / workers
			endY := (worker + 1) * p.ImageHeight / workers
			//splite
			wg.Add(1)
			go func(startY,endY int) {
				defer wg.Done()
				//calculate of course
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
		world = nextWorld
		turn++
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
