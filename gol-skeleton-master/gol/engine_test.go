//to be honest, I create this file with ai

package gol

import (
	"errors"
	"net"
	"net/rpc"
	"reflect"
	"testing"
	"time"
)

func TestEngineCompletesRequestedTurns(t *testing.T) {
	engine := NewEngine()
	world := make([][]uint8, 8)
	for y := range world {
		world[y] = make([]uint8, 8)
	}
	world[3][2] = 255
	world[3][3] = 255
	world[3][4] = 255

	var startReply StartReply
	if err := engine.StartEvolution(EvolveRequest{
		World: world, Turns: 1, Threads: 2,
	}, &startReply); err != nil {
		t.Fatal(err)
	}
	if !startReply.Started {
		t.Fatal("expected evolution to start")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var snapshot Snapshot
		if err := engine.GetSnapshot(Empty{}, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.Finished {
			if snapshot.CompletedTurns != 1 {
				t.Fatalf("completed turns = %d, want 1", snapshot.CompletedTurns)
			}
			if want := evolveOneTurn(world, 2); !reflect.DeepEqual(snapshot.World, want) {
				t.Fatal("engine returned an unexpected world")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("engine did not finish in time")
}

func TestRunGameRPCReturnsFinalWorld(t *testing.T) {
	engine := NewEngine()
	world := make([][]uint8, 8)
	for y := range world {
		world[y] = make([]uint8, 8)
	}
	world[3][2] = 255
	world[3][3] = 255
	world[3][4] = 255

	server := rpc.NewServer()
	if err := server.RegisterName("GolEngine", engine); err != nil {
		t.Fatal(err)
	}
	clientConn, serverConn := net.Pipe()
	go server.ServeConn(serverConn)
	client := rpc.NewClient(clientConn)
	t.Cleanup(func() {
		_ = client.Close()
	})

	var result GameResult
	if err := client.Call("GolEngine.RunGame", EvolveRequest{
		World: world, Turns: 1, Threads: 2,
	}, &result); err != nil {
		t.Fatal(err)
	}
	if result.CompletedTurns != 1 {
		t.Fatalf("completed turns = %d, want 1", result.CompletedTurns)
	}
	if want := evolveOneTurn(world, 2); !reflect.DeepEqual(result.World, want) {
		t.Fatal("RunGame returned an unexpected final world")
	}
}

func TestRunGameCanRunSequentialGames(t *testing.T) {
	engine := NewEngine()
	worker := NewWorker()
	if err := engine.SetWorkers([]string{startTestWorkerServer(t, worker)}); err != nil {
		t.Fatal(err)
	}

	firstWorld := make([][]uint8, 4)
	secondWorld := make([][]uint8, 4)
	for y := range firstWorld {
		firstWorld[y] = make([]uint8, 4)
		secondWorld[y] = make([]uint8, 4)
	}
	firstWorld[1][0] = 255
	firstWorld[1][1] = 255
	firstWorld[1][2] = 255
	secondWorld[0][0] = 255
	secondWorld[2][2] = 255

	requests := []EvolveRequest{
		{World: firstWorld, Turns: 1, Threads: 2},
		{World: secondWorld, Turns: 2, Threads: 1},
	}
	for i, request := range requests {
		var result GameResult
		if err := engine.RunGame(request, &result); err != nil {
			t.Fatalf("RunGame %d failed: %v", i+1, err)
		}
		if result.CompletedTurns != request.Turns {
			t.Fatalf("RunGame %d completed turns = %d, want %d", i+1, result.CompletedTurns, request.Turns)
		}

		want := cloneWorld(request.World)
		for turn := 0; turn < request.Turns; turn++ {
			want = evolveOneTurn(want, request.Threads)
		}
		if !reflect.DeepEqual(result.World, want) {
			t.Fatalf("RunGame %d returned an unexpected world", i+1)
		}
	}

	var shutdownReply Empty
	if err := engine.Shutdown(Empty{}, &shutdownReply); err != nil {
		t.Fatal(err)
	}
}

func TestRunGameReplacesActiveGame(t *testing.T) {
	engine := NewEngine()
	firstWorld := make([][]uint8, 64)
	for y := range firstWorld {
		firstWorld[y] = make([]uint8, 64)
	}

	firstResult := make(chan error, 1)
	go func() {
		var result GameResult
		firstResult <- engine.RunGame(EvolveRequest{
			World: firstWorld,
			Turns: 1_000_000,
		}, &result)
	}()

	deadline := time.Now().Add(time.Second)
	activeGameObserved := false
	for time.Now().Before(deadline) {
		var snapshot Snapshot
		if err := engine.GetSnapshot(Empty{}, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.CompletedTurns > 0 {
			activeGameObserved = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !activeGameObserved {
		t.Fatal("first game did not start before the deadline")
	}

	secondWorld := [][]uint8{{255, 0}, {0, 255}}
	var secondResult GameResult
	if err := engine.RunGame(EvolveRequest{
		World: secondWorld,
		Turns: 1,
	}, &secondResult); err != nil {
		t.Fatal(err)
	}
	if secondResult.CompletedTurns != 1 {
		t.Fatalf("second game completed turns = %d, want 1", secondResult.CompletedTurns)
	}
	if want := evolveOneTurn(secondWorld, 1); !reflect.DeepEqual(secondResult.World, want) {
		t.Fatal("second game returned an unexpected world")
	}
	if err := <-firstResult; err == nil {
		t.Fatal("replaced game did not return an error")
	}
}

func TestRunGameCanStartPaused(t *testing.T) {
	engine := NewEngine()
	world := make([][]uint8, 8)
	for y := range world {
		world[y] = make([]uint8, 8)
	}

	resultCh := make(chan error, 1)
	go func() {
		var result GameResult
		resultCh <- engine.RunGame(EvolveRequest{
			World:       world,
			Turns:       2,
			Threads:     2,
			GameID:      "start-paused",
			StartPaused: true,
		}, &result)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var snapshot Snapshot
		if err := engine.GetSnapshot(Empty{}, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.GameID == "start-paused" && snapshot.Paused {
			if snapshot.CompletedTurns != 0 {
				t.Fatalf("completed turns while initially paused = %d, want 0", snapshot.CompletedTurns)
			}
			break
		}
		time.Sleep(time.Millisecond)
	}

	var snapshot Snapshot
	if err := engine.GetSnapshot(Empty{}, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.GameID != "start-paused" || !snapshot.Paused {
		t.Fatal("engine did not enter the requested initial paused state")
	}
	var resumeReply Snapshot
	if err := engine.Resume(Empty{}, &resumeReply); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("game did not finish after resuming")
	}
}

func TestEngineCanPauseResumeAndShutdown(t *testing.T) {
	engine := NewEngine()
	world := make([][]uint8, 64)
	for y := range world {
		world[y] = make([]uint8, 64)
	}

	var startReply StartReply
	if err := engine.StartEvolution(EvolveRequest{
		World: world, Turns: 1_000_000, Threads: 4,
	}, &startReply); err != nil {
		t.Fatal(err)
	}

	var paused Snapshot
	if err := engine.Pause(Empty{}, &paused); err != nil {
		t.Fatal(err)
	}
	if !paused.Paused {
		t.Fatal("engine did not pause")
	}

	var resumed Snapshot
	if err := engine.Resume(Empty{}, &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.Paused {
		t.Fatal("engine did not resume")
	}

	if err := engine.Pause(Empty{}, &paused); err != nil {
		t.Fatal(err)
	}
	var shutdownReply Empty
	if err := engine.Shutdown(Empty{}, &shutdownReply); err != nil {
		t.Fatal(err)
	}

	var final Snapshot
	if err := engine.GetSnapshot(Empty{}, &final); err != nil {
		t.Fatal(err)
	}
	if !final.Finished {
		t.Fatal("engine did not finish after shutdown")
	}
	select {
	case <-engine.StopSignal():
	default:
		t.Fatal("shutdown signal was not sent")
	}
}

type failingTestWorker struct{}

func (failingTestWorker) ComputeRows(_ ComputeRowsRequest, _ *ComputeRowsReply) error {
	return errors.New("simulated worker failure")
}

func (failingTestWorker) Shutdown(_ Empty, _ *Empty) error {
	return nil
}

func startTestWorkerServer(t *testing.T, worker interface{}) string {
	t.Helper()

	server := rpc.NewServer()
	if err := server.RegisterName("GolWorker", worker); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	go func() {
		conn, err := listener.Accept()
		if err == nil {
			server.ServeConn(conn)
		}
	}()
	return listener.Addr().String()
}

func TestEngineReportsWorkerFailure(t *testing.T) {
	engine := NewEngine()
	if err := engine.SetWorkers([]string{startTestWorkerServer(t, failingTestWorker{})}); err != nil {
		t.Fatal(err)
	}

	var result GameResult
	if err := engine.RunGame(EvolveRequest{
		World: [][]uint8{{255, 0}, {0, 255}},
		Turns: 1,
	}, &result); err == nil {
		t.Fatal("RunGame did not return the worker failure")
	}

	var snapshot Snapshot
	if err := engine.GetSnapshot(Empty{}, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Error == "" {
		t.Fatal("snapshot did not report the worker failure")
	}
	var countReply AliveCountReply
	if err := engine.GetAliveCells(AliveCountRequest{}, &countReply); err != nil {
		t.Fatal(err)
	}
	if countReply.Error == "" {
		t.Fatal("alive-cell response did not report the worker failure")
	}
	var shutdownReply Empty
	if err := engine.Shutdown(Empty{}, &shutdownReply); err != nil {
		t.Fatal(err)
	}
}

func TestEngineShutdownStopsWorkers(t *testing.T) {
	worker := NewWorker()
	engine := NewEngine()
	if err := engine.SetWorkers([]string{startTestWorkerServer(t, worker)}); err != nil {
		t.Fatal(err)
	}

	var startReply StartReply
	if err := engine.StartEvolution(EvolveRequest{
		World: [][]uint8{{0}},
		Turns: 0,
	}, &startReply); err != nil {
		t.Fatal(err)
	}

	var shutdownReply Empty
	if err := engine.Shutdown(Empty{}, &shutdownReply); err != nil {
		t.Fatal(err)
	}
	select {
	case <-worker.StopSignal():
	default:
		t.Fatal("Engine shutdown did not stop the worker")
	}
}
