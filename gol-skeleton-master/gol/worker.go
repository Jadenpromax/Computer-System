package gol

import (
	"fmt"
	"sync"
)

type ComputeRowsRequest struct {
	JobID  string
	Turn   int
	StartY int
	Width  int
	Height int

	TopHalo    []uint8
	Rows       [][]uint8
	BottomHalo []uint8
}

type ComputeRowsReply struct {
	JobID  string
	Turn   int
	StartY int
	Rows   [][]uint8
}

type Worker struct {
	stopSignal chan struct{}
	stopOnce   sync.Once
}

func NewWorker() *Worker {
	return &Worker{stopSignal: make(chan struct{})}
}

func (w *Worker) StopSignal() <-chan struct{} {
	return w.stopSignal
}

func (w *Worker) Shutdown(_ Empty, _ *Empty) error {
	w.stopOnce.Do(func() {
		close(w.stopSignal)
	})
	return nil
}

func (w *Worker) ComputeRows(req ComputeRowsRequest, reply *ComputeRowsReply) error {
	if req.JobID == "" {
		return fmt.Errorf("job ID is required")
	}
	if req.Width <= 0 || req.Height <= 0 {
		return fmt.Errorf("board dimensions must be positive")
	}
	if len(req.Rows) == 0 || req.StartY < 0 || req.StartY+len(req.Rows) > req.Height {
		return fmt.Errorf("invalid row range")
	}
	if len(req.TopHalo) != req.Width || len(req.BottomHalo) != req.Width {
		return fmt.Errorf("halo rows have incorrect width")
	}
	for _, row := range req.Rows {
		if len(row) != req.Width {
			return fmt.Errorf("input row has incorrect width")
		}
	}

	rowAt := func(y int) []uint8 {
		switch {
		case y < 0:
			return req.TopHalo
		case y >= len(req.Rows):
			return req.BottomHalo
		default:
			return req.Rows[y]
		}
	}

	next := make([][]uint8, len(req.Rows))
	for y := range req.Rows {
		next[y] = make([]uint8, req.Width)
		for x := 0; x < req.Width; x++ {
			neighbors := 0
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					if dx == 0 && dy == 0 {
						continue
					}

					nx := (x + dx + req.Width) % req.Width
					if rowAt(y + dy)[nx] == 255 {
						neighbors++
					}
				}
			}

			if req.Rows[y][x] == 255 {
				if neighbors == 2 || neighbors == 3 {
					next[y][x] = 255
				}
			} else if neighbors == 3 {
				next[y][x] = 255
			}
		}
	}

	reply.JobID = req.JobID
	reply.Turn = req.Turn
	reply.StartY = req.StartY
	reply.Rows = next
	return nil
}
