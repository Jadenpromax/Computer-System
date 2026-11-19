package gol

import (
	"fmt"
	"sync"
)

type EvolveRequest struct {
	World   [][]uint8
	Turns   int
	Threads int
}

// Empty is an empty struct used for RPC methods that do not require any input parameters.
type Empty struct{}

type StartReply struct {
	Started bool
}

type Engine struct {
	mu               sync.Mutex
	world            [][]uint8
	completedTurns   int
	cond             *sync.Cond
	targetTurns      int
	threads          int
	started          bool
	paused           bool
	pausedAtBoundary bool
	finished         bool
	stopping         bool
	stopSignal       chan struct{}
	stopSignalClosed bool
}

type Snapshot struct {
	World          [][]uint8
	CompletedTurns int
	Paused         bool
	Finished       bool
}

type AliveCountRequest struct{}

type AliveCountReply struct {
	CompletedTurns int
	CellsCount     int
	World          [][]uint8
	Paused         bool
	Finished       bool
}

// moved from controller to engine
func countAliveCells(world [][]uint8) int {
	count := 0
	for y := range world {
		for x := range world[y] {
			if world[y][x] == 255 {
				count++
			}
		}
	}
	return count
}

// GetAliveCells returns the number of alive cells in the current world state.
func (e *Engine) GetAliveCells(_ AliveCountRequest, reply *AliveCountReply) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	reply.CompletedTurns = e.completedTurns
	reply.CellsCount = countAliveCells(e.world)
	reply.World = cloneWorld(e.world)
	reply.Paused = e.paused
	reply.Finished = e.finished
	return nil
}

func cloneWorld(world [][]uint8) [][]uint8 {
	copyOfWorld := make([][]uint8, len(world))
	for y := range world {
		copyOfWorld[y] = append([]uint8(nil), world[y]...)
	}
	return copyOfWorld
}

func (e *Engine) GetSnapshot(_ Empty, reply *Snapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	reply.World = cloneWorld(e.world)
	reply.CompletedTurns = e.completedTurns
	reply.Paused = e.paused
	reply.Finished = e.finished
	return nil
}

func NewEngine() *Engine {
	e := &Engine{stopSignal: make(chan struct{})}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *Engine) StopSignal() <-chan struct{} {
	return e.stopSignal
}

func (e *Engine) StartEvolution(req EvolveRequest, reply *StartReply) error {
	if req.Turns < 0 {
		return fmt.Errorf("turns cannot be negative")
	}
	if len(req.World) == 0 || len(req.World[0]) == 0 {
		return fmt.Errorf("world cannot be empty")
	}
	width := len(req.World[0])
	for _, row := range req.World {
		if len(row) != width {
			return fmt.Errorf("world rows have inconsistent widths")
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopping {
		return fmt.Errorf("engine is shutting down")
	}
	if e.started {
		return nil
	}

	e.world = cloneWorld(req.World)
	e.targetTurns = req.Turns
	e.threads = req.Threads
	e.completedTurns = 0
	e.started = true
	reply.Started = true
	go e.run()
	return nil
}

func (e *Engine) run() {
	for {
		e.mu.Lock()
		for e.paused && !e.stopping {
			e.pausedAtBoundary = true
			e.cond.Broadcast()
			e.cond.Wait()
		}
		e.pausedAtBoundary = false

		if e.stopping || e.completedTurns >= e.targetTurns {
			e.finished = true
			e.cond.Broadcast()
			e.mu.Unlock()
			return
		}
        //caculate first and then update the world and completedTurns
		world := cloneWorld(e.world)
		threads := e.threads
		nextTurn := e.completedTurns + 1
		e.mu.Unlock()

		next := evolveOneTurn(world, threads)

		e.mu.Lock()
		e.world = next
		e.completedTurns = nextTurn
		e.cond.Broadcast()
		e.mu.Unlock()
	}
}

func (e *Engine) Pause(_ Empty, reply *Snapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.finished || e.stopping {
		*reply = Snapshot{
			World:          cloneWorld(e.world),
			CompletedTurns: e.completedTurns,
			Paused:         e.paused,
			Finished:       e.finished,
		}
		return nil
	}
	e.paused = true
	for !e.pausedAtBoundary && !e.finished && !e.stopping {
		e.cond.Wait()
	}
	*reply = Snapshot{
		World:          cloneWorld(e.world),
		CompletedTurns: e.completedTurns,
		Paused:         e.paused,
		Finished:       e.finished,
	}
	return nil
}

func (e *Engine) Resume(_ Empty, reply *Snapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.finished || e.stopping {
		*reply = Snapshot{
			World:          cloneWorld(e.world),
			CompletedTurns: e.completedTurns,
			Paused:         e.paused,
			Finished:       e.finished,
		}
		return nil
	}
	e.paused = false
	e.pausedAtBoundary = false
	e.cond.Broadcast()
	*reply = Snapshot{
		World:          cloneWorld(e.world),
		CompletedTurns: e.completedTurns,
		Paused:         false,
		Finished:       e.finished,
	}
	return nil
}

func (e *Engine) Shutdown(_ Empty, reply *Empty) error {
	e.mu.Lock()
	if !e.stopping {
		e.stopping = true
		e.paused = false
		if !e.stopSignalClosed {
			close(e.stopSignal)
			e.stopSignalClosed = true
		}
		e.cond.Broadcast()
	}
	for e.started && !e.finished {
		e.cond.Wait()
	}
	e.mu.Unlock()
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
