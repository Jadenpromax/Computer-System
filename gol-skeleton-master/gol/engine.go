package gol
import (
	"fmt"
	"uk.ac.bris.cs/gameoflife/util"
	"sync"
)

type EvolveRequest struct {
	World [][]uint8
	Turns int
	Threads int
}

type TurnResult struct {
	CompletedTurns int
	Flipped []util.Cell

}

type EvolveReply struct {
	World [][]uint8
	Result []TurnResult
}

type Engine struct {}

func (e *Engine) Evolve(req EvolveRequest, reply *EvolveReply) error {
    if req.Turns < 0 {
        return fmt.Errorf("turns cannot be negative")
    }
    if len(req.World) == 0 || len(req.World[0]) == 0 {
        return fmt.Errorf("world cannot be empty")
    }

    world := req.World
    for turn := 0; turn < req.Turns; turn++ {
        next := evolveOneTurn(world, req.Threads)

        flipped := make([]util.Cell, 0)
        for y := range world {
            for x := range world[y] {
                if world[y][x] != next[y][x] {
                    flipped = append(flipped, util.Cell{X: x, Y: y})
                }
            }
        }

        world = next
        reply.Result = append(reply.Result, TurnResult{
            CompletedTurns: turn + 1,
            Flipped:        flipped,
        })
    }

    reply.World = world
    return nil
}


func evolveOneTurn(world [][]uint8, threads int) [][]uint8 {
    height := len(world)
    if height == 0 {
        return nil
    }

    width := len(world[0])
    if width == 0 {
        return nil
    }

    if threads < 1 {
        threads = 1
    }
    if threads > height {
        threads = height
    }

    next := make([][]uint8, height)
    for y := range next {
        next[y] = make([]uint8, width)
    }

    var wg sync.WaitGroup

    for worker := 0; worker < threads; worker++ {
        startY := worker * height / threads
        endY := (worker + 1) * height / threads
		//splite↑
        wg.Add(1)
        go func(startY, endY int) {
            defer wg.Done()

            for y := startY; y < endY; y++ {
                for x := 0; x < width; x++ {
                    aliveNeighbours := 0

                    for dy := -1; dy <= 1; dy++ {
                        for dx := -1; dx <= 1; dx++ {
                            if dx == 0 && dy == 0 {
                                continue
                            }

                            nx := (x + dx + width) % width
                            ny := (y + dy + height) % height
                            if world[ny][nx] == 255 {
                                aliveNeighbours++
                            }
                        }
                    }

                    if world[y][x] == 255 {
                        if aliveNeighbours == 2 || aliveNeighbours == 3 {
                            next[y][x] = 255
                        }
                    } else if aliveNeighbours == 3 {
                        next[y][x] = 255
                    }
                }
            }
        }(startY, endY)
    }

    wg.Wait()
    return next
}