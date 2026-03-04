# MIT 6.5840 Distributed Systems

Lab implementations for [MIT 6.5840 (Spring 2025)](http://nil.csail.mit.edu/6.5840/2025/), formerly 6.824.

## Lab 1 — MapReduce

A distributed MapReduce system modeled after the [original Google paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

### Architecture

```
                  ┌─────────────────┐
                  │   Coordinator   │
                  │  (task queue)   │
                  └────────┬────────┘
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
     ┌─────────┐      ┌─────────┐      ┌─────────┐
     │ Worker  │      │ Worker  │      │ Worker  │
     └─────────┘      └─────────┘      └─────────┘
```

Workers communicate with the coordinator via **RPC over Unix domain sockets**. The coordinator distributes map and reduce tasks; workers execute them and report completion.

### What's implemented

**Coordinator** (`src/mr/coordinator.go`)
- Buffered channels as task queues for map and reduce phases
- Background goroutine for timeout detection — re-queues tasks from workers that take longer than 10 seconds
- Phase-gated execution: reduce tasks only become available after all map tasks are confirmed complete
- `sync.Mutex`-protected state with a `done` channel for job-completion signaling

**Worker** (`src/mr/worker.go`)
- Continuous task loop: request → execute → report done
- Map execution: reads input file, calls the user-supplied `mapf`, routes key-value pairs into `nReduce` intermediate files via `ihash(key) % nReduce`, writes atomically using temp-file-then-rename
- Reduce execution: reads all intermediate files for its partition, sorts by key, groups values, calls `reducef`, writes output atomically

**RPC definitions** (`src/mr/rpc.go`)
- `GetTask` / `ReportDone` RPC interface between workers and coordinator
- Task type constants (`MapTask`, `ReduceTask`, `WaitTask`, `DoneTask`)

### Test results

```
--- wc test: PASS
--- indexer test: PASS
--- map parallelism test: PASS
--- reduce parallelism test: PASS
--- job count test: PASS
--- early exit test: PASS
--- crash test: PASS
*** PASSED ALL TESTS
```

### Running

```bash
# Build the word-count plugin
cd src/mrapps && go build -race -buildmode=plugin wc.go

# Terminal 1 — start coordinator
cd src/main && go run -race mrcoordinator.go pg-*.txt

# Terminal 2+ — start workers
cd src/main && go run -race mrworker.go wc.so

# Run the full test suite
cd src/main && bash test-mr.sh
```
