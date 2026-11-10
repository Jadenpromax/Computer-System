package gol

import (
	"fmt"

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
	for turn < p.Turns {
		nextWorld := make([][]uint8, p.ImageHeight)

		for y := 0; y < p.ImageHeight; y++ {
			nextWorld[y] = make([]uint8, p.ImageWidth)
		}

		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				aliveNeighbours := 0

				// 检查周围八个位置
				for dy := -1; dy <= 1; dy++ {
					for dx := -1; dx <= 1; dx++ {
						// 不统计当前细胞自己
						if dx == 0 && dy == 0 {
							continue
						}

						// 环绕边界
						neighbourX := (x + dx + p.ImageWidth) % p.ImageWidth
						neighbourY := (y + dy + p.ImageHeight) % p.ImageHeight

						if world[neighbourY][neighbourX] == 255 {
							aliveNeighbours++
						}
					}
				}

				// 当前细胞是活细胞
				if world[y][x] == 255 {
					if aliveNeighbours == 2 || aliveNeighbours == 3 {
						nextWorld[y][x] = 255
					} else {
						nextWorld[y][x] = 0
					}
				} else {
					// 当前细胞是死细胞
					if aliveNeighbours == 3 {
						nextWorld[y][x] = 255
					} else {
						nextWorld[y][x] = 0
					}
				}
			}
		}

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
