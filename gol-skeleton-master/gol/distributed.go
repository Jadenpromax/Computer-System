package gol

import (
    "fmt"
    "net/rpc"
    "time"
)

type workerTask struct {
    client *rpc.Client
    start  int
    end    int
}

type workerResult struct {
    task  workerTask
    reply ComputeRowsReply
    err   error
}

// distributedEvolveOneTurn computes one generation across the workers.
// It returns no board if any worker fails or returns invalid rows.
func distributedEvolveOneTurn(
    world [][]uint8,
    turn int,
    clients []*rpc.Client,
) ([][]uint8, error) {
    height := len(world)
    if height == 0 || len(world[0]) == 0 {
        return nil, fmt.Errorf("world cannot be empty")
    }
    if len(clients) == 0 {
        return nil, fmt.Errorf("no workers configured")
    }

    width := len(world[0])
    for _, row := range world {
        if len(row) != width {
            return nil, fmt.Errorf("world rows have inconsistent widths")
        }
    }

    workerCount := len(clients)
    if workerCount > height {
        workerCount = height
    }

    jobID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), turn)
    results := make(chan workerResult, workerCount)
	// Partition the board into contiguous, nearly equal row ranges [start, end),
	// and assign one range to each worker.
    for i := 0; i < workerCount; i++ {
        start := i * height / workerCount
        end := (i + 1) * height / workerCount
        task := workerTask{
            client: clients[i],
            start:  start,
            end:    end,
        }

        req := ComputeRowsRequest{
            JobID:      jobID,
            Turn:       turn,
            StartY:     start,
            Width:      width,
            Height:     height,
            TopHalo:    append([]uint8(nil), world[(start-1+height)%height]...),
            Rows:       cloneWorld(world[start:end]),
            BottomHalo: append([]uint8(nil), world[end%height]...),
        }

        go func(task workerTask, req ComputeRowsRequest) {
            var reply ComputeRowsReply
            err := task.client.Call("GolWorker.ComputeRows", req, &reply)
            results <- workerResult{task: task, reply: reply, err: err}
        }(task, req)
    }

    next := make([][]uint8, height)
    received := make([]bool, height)
    var firstErr error

    for i := 0; i < workerCount; i++ {
        result := <-results
        if result.err != nil {
            if firstErr == nil {
                firstErr = fmt.Errorf("worker rows [%d,%d): %w",
                    result.task.start, result.task.end, result.err)
            }
            continue
        }

        reply := result.reply
        if reply.JobID != jobID ||
            reply.Turn != turn ||
            reply.StartY != result.task.start ||
            len(reply.Rows) != result.task.end-result.task.start {
            if firstErr == nil {
                firstErr = fmt.Errorf("worker returned mismatched task metadata")
            }
            continue
        }

        valid := true
        for _, row := range reply.Rows {
            if len(row) != width {
                valid = false
                break
            }
        }
        if !valid {
            if firstErr == nil {
                firstErr = fmt.Errorf("worker returned a row with incorrect width")
            }
            continue
        }

        for offset, row := range reply.Rows {
            y := result.task.start + offset
            if received[y] {
                if firstErr == nil {
                    firstErr = fmt.Errorf("worker returned duplicate row %d", y)
                }
                continue
            }
            next[y] = row
            received[y] = true
        }
    }

    if firstErr != nil {
        return nil, firstErr
    }
	// check if the result is complete
    for y, ok := range received {
        if !ok {
            return nil, fmt.Errorf("worker results omitted row %d", y)
        }
    }
    return next, nil
}