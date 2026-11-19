//to be honest, I create this file with ai cuz IDK how to test if engine works properly

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
