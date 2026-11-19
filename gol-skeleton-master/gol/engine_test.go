package gol

import (
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
