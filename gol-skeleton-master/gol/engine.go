package gol

import (
	"fmt"
	"net/rpc"
	"strings"
	"sync"
)

type EvolveRequest struct {
	World       [][]uint8
	Turns       int
	Threads     int
	GameID      string
	StartPaused bool
}

// Empty is an empty struct used for RPC methods that do not require any input parameters.
type Empty struct{}

type StartReply struct {
	Started bool
}

type GameResult struct {
	World          [][]uint8
	CompletedTurns int
}

type gameRun struct {
	done           chan struct{}
	world          [][]uint8
	completedTurns int
	err            error
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
	workerClients    []*rpc.Client
	runError         string
	currentRun       *gameRun
	gameID           string
}

type Snapshot struct {
	World          [][]uint8
	CompletedTurns int
	Paused         bool
	Finished       bool
	Error          string
	GameID         string
}

type AliveCountRequest struct{}

type AliveCountReply struct {
	CompletedTurns int
	CellsCount     int
	World          [][]uint8
	Paused         bool
	Finished       bool
	Error          string
	GameID         string
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
	reply.Error = e.runError
	reply.GameID = e.gameID
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
	reply.Error = e.runError
	reply.GameID = e.gameID
	return nil
}

// initialization
func NewEngine() *Engine {
	e := &Engine{stopSignal: make(chan struct{})}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *Engine) StopSignal() <-chan struct{} {
	return e.stopSignal
}

func (e *Engine) SetWorkers(addresses []string) error {
	clients := make([]*rpc.Client, 0, len(addresses))
	for _, address := range addresses {
		address = strings.TrimSpace(address)
		//skip blank address
		if address == "" {
			continue
		}

		client, err := rpc.Dial("tcp", address)
		if err != nil {
			for _, opened := range clients {
				_ = opened.Close()
			}
			return fmt.Errorf("connect to worker %s: %w", address, err)
		}
		clients = append(clients, client)
	}
	if len(clients) == 0 {
		return fmt.Errorf("no worker addresses configured")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started || e.stopping {
		for _, client := range clients {
			_ = client.Close()
		}
		return fmt.Errorf("workers must be configured before evolution starts")
	}
	e.workerClients = clients
	return nil
}

func (e *Engine) StartEvolution(req EvolveRequest, reply *StartReply) error {
	if err := validateEvolveRequest(req); err != nil {
		return err
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
	e.paused = req.StartPaused
	e.pausedAtBoundary = false
	e.gameID = req.GameID
	e.started = true
	run := &gameRun{done: make(chan struct{})}
	e.currentRun = run
	reply.Started = true
	go e.run(run)
	return nil
}

func validateEvolveRequest(req EvolveRequest) error {
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
	return nil
}

// main RPC for Broker
func (e *Engine) RunGame(req EvolveRequest, reply *GameResult) error {
	if err := validateEvolveRequest(req); err != nil {
		return err
	}

	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return fmt.Errorf("engine is shutting down")
	}
	if e.currentRun != nil && !e.finished {
		e.currentRun.world = cloneWorld(e.world)
		e.currentRun.completedTurns = e.completedTurns
		e.currentRun.err = fmt.Errorf("game was replaced by a newer request")
		close(e.currentRun.done)
	}

	run := &gameRun{done: make(chan struct{})}
	e.currentRun = run
	e.world = cloneWorld(req.World)
	e.targetTurns = req.Turns
	e.threads = req.Threads
	e.completedTurns = 0
	e.paused = req.StartPaused
	e.pausedAtBoundary = false
	e.finished = false
	e.runError = ""
	e.gameID = req.GameID
	e.started = true
	e.cond.Broadcast()
	go e.run(run)
	e.mu.Unlock()

	<-run.done
	reply.World = cloneWorld(run.world)
	reply.CompletedTurns = run.completedTurns
	if run.err != nil {
		return run.err
	}
	return nil
}

func (e *Engine) finishRunLocked(run *gameRun, err error) {
	if e.currentRun != run {
		return
	}
	e.finished = true
	if err != nil {
		e.runError = err.Error()
	}
	run.world = cloneWorld(e.world)
	run.completedTurns = e.completedTurns
	run.err = err
	close(run.done)
	e.cond.Broadcast()
}

func (e *Engine) run(run *gameRun) {
	for {
		e.mu.Lock()
		if e.currentRun != run {
			e.mu.Unlock()
			return
		}
		for e.paused && !e.stopping && e.currentRun == run {
			e.pausedAtBoundary = true
			e.cond.Broadcast()
			e.cond.Wait()
		}
		if e.currentRun != run {
			e.mu.Unlock()
			return
		}
		e.pausedAtBoundary = false

		if e.stopping || e.completedTurns >= e.targetTurns {
			e.finishRunLocked(run, nil)
			e.mu.Unlock()
			return
		}
		//caculate first and then update the world and completedTurns
		world := cloneWorld(e.world)
		threads := e.threads
		nextTurn := e.completedTurns + 1
		clients := append([]*rpc.Client(nil), e.workerClients...)
		e.mu.Unlock()

		var next [][]uint8
		var err error

		if len(clients) == 0 {
			// 本机模式，方便对照测试。
			next = evolveOneTurn(world, threads)
		} else {
			next, err = distributedEvolveOneTurn(world, nextTurn, clients)
		}

		if err != nil {
			e.mu.Lock()
			e.finishRunLocked(run, err)
			e.mu.Unlock()
			return
		}

		e.mu.Lock()
		if e.currentRun != run {
			e.mu.Unlock()
			return
		}
		e.world = next
		e.completedTurns = nextTurn
		e.cond.Broadcast()
		e.mu.Unlock()
	}
}

// implement of different keyPresses
func (e *Engine) Pause(_ Empty, reply *Snapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.finished || e.stopping {
		*reply = Snapshot{
			World:          cloneWorld(e.world),
			CompletedTurns: e.completedTurns,
			Paused:         e.paused,
			Finished:       e.finished,
			Error:          e.runError,
			GameID:         e.gameID,
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
		Error:          e.runError,
		GameID:         e.gameID,
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
			Error:          e.runError,
			GameID:         e.gameID,
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
		Error:          e.runError,
		GameID:         e.gameID,
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
	workerClients := e.workerClients
	e.workerClients = nil
	e.mu.Unlock()

	var shutdownErr error
	for _, client := range workerClients {
		var workerReply Empty
		if err := client.Call("GolWorker.Shutdown", Empty{}, &workerReply); err != nil && shutdownErr == nil {
			shutdownErr = fmt.Errorf("shut down worker: %w", err)
		}
		if err := client.Close(); err != nil && shutdownErr == nil {
			shutdownErr = fmt.Errorf("close worker connection: %w", err)
		}
	}
	return shutdownErr
}

// the parallel caculation method
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
