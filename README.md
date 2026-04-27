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

---

## Lab 2 — Key/Value Server with Fault Tolerance

A fault-tolerant key/value server with **optimistic concurrency control** via versioned puts, and a **distributed lock** built on top of it. Designed to handle dropped RPC requests and replies on unreliable networks.

### Architecture

```
┌─────────────────────────────────────────┐
│              Application                │
│   clerk.Put(key, val, ver)              │
│   clerk.Get(key)                        │
│   lock.Acquire() / lock.Release()       │
└──────────────┬──────────────────────────┘
               │ RPC (unreliable network)
               ▼
┌─────────────────────────────────────────┐
│             KVServer                    │
│   map[string]→(value, version)          │
│   versioned Put = compare-and-swap      │
└─────────────────────────────────────────┘
```

### What's implemented

**KV Server** (`src/kvsrv1/server.go`)
- In-memory map storing `(value, version)` per key
- `Get` returns current value and version; `ErrNoKey` if absent
- `Put` is a **compare-and-swap**: only applies if the client's version matches the server's current version, then increments it — returns `ErrVersion` on mismatch
- All operations protected by `sync.Mutex` for concurrent client safety

**Clerk** (`src/kvsrv1/client.go`)
- `Get`: retries forever until a reply is received (safe — reads are idempotent)
- `Put`: retries on dropped messages, distinguishing between first attempt and retries:
  - `ErrVersion` on first attempt → returns `ErrVersion` (Put definitely did not execute)
  - `ErrVersion` on a retry → returns `ErrMaybe` (Put may have executed; reply was dropped)

**Distributed Lock** (`src/kvsrv1/lock/lock.go`)
- Built entirely on `Clerk.Put` and `Clerk.Get` — no server-side lock logic
- Uses the lock key's value as state: `""` = free, `clientID` = held
- `Acquire`: spins doing `Get` → `Put(myID)`, handles `ErrMaybe` by re-checking the value
- `Release`: loops until `Get` confirms the key is `""`, handles dropped Put replies

### Key design insight

The versioned `Put` is a **distributed compare-and-swap**. Two clients racing to acquire the lock both see `val=""` and both attempt `Put(myID, ver)` — but only one has the correct version after the first winner's write. The loser gets `ErrVersion` and retries. No server-side lock logic is needed; mutual exclusion falls out of the CAS primitive.

### Test results

```
# KV Server tests
$ cd src/kvsrv1 && go test -race -v
--- PASS: TestReliablePut
--- PASS: TestPutConcurrentReliable
--- PASS: TestMemPutManyClientsReliable
--- PASS: TestUnreliableNet
PASS

# Distributed lock tests
$ cd src/kvsrv1/lock && go test -race -v
--- PASS: TestOneClientReliable
--- PASS: TestManyClientsReliable
--- PASS: TestOneClientUnreliable
--- PASS: TestManyClientsUnreliable
PASS
```

### Running

```bash
# KV server tests
cd src/kvsrv1 && go test -race -v

# Lock tests
cd src/kvsrv1/lock && go test -race -v
```

---

## Lab 3 — Raft Consensus

A full implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) covering leader election, log replication, persistence, and log compaction via snapshots.

### Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Service Layer (KV server)              │
│          applyCh ◄──── committed entries / snapshots      │
│          Snapshot() ──► trim Raft log                     │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│                       Raft Layer                         │
│                                                          │
│  ┌──────────┐    AppendEntries RPC    ┌──────────┐       │
│  │  Leader   │ ─────────────────────► │ Follower │       │
│  │          │ ◄───────────────────── │          │       │
│  │          │  InstallSnapshot RPC   │          │       │
│  │          │ ─────────────────────► │          │       │
│  └──────────┘                        └──────────┘       │
│       ▲                                                  │
│       │ RequestVote RPC                                  │
│  ┌──────────┐                                            │
│  │Candidate │                                            │
│  └──────────┘                                            │
└──────────────────────────────────────────────────────────┘
```

### What's implemented

**Lab 3A — Leader Election** (`src/raft1/raft.go`)
- Randomized election timeouts (300–500ms) to avoid split votes
- Term-based voting with the election restriction (Section 5.4.1) — candidates must have an up-to-date log to win
- Periodic heartbeats (100ms) from the leader to suppress elections
- `stepDown()` helper for consistent term/role transitions

**Lab 3B — Log Replication**
- `AppendEntries` RPC handler with the Log Matching Property: verifies `PrevLogIndex`/`PrevLogTerm` before accepting entries
- Fast backup optimization with `ConflictTerm`/`ConflictIndex` hints — skips entire conflicting terms instead of decrementing `nextIndex` one at a time
- `advanceCommitIndex()` only commits entries from the current term (Figure 8 safety)
- Dedicated `applier()` goroutine delivers committed entries to the service layer via `applyCh`

**Lab 3C — Persistence**
- `persist()` / `readPersist()` using `labgob` encoding for `currentTerm`, `votedFor`, `log`, and snapshot metadata
- `encodeRaftState()` helper to avoid duplication between `persist()` and `Snapshot()`
- Persistence calls at every state-mutation point: vote grants, term changes, log appends, log truncations

**Lab 3D — Log Compaction (Snapshots)**
- `Snapshot()`: trims the log and persists both Raft state and snapshot bytes atomically
- Offset-aware indexing via `toSlicePos()` and `lastLogIndex()` — translates between Raft indices and slice positions
- Anchor entry at `log[0]` preserves the term of the last snapshotted entry
- `InstallSnapshot` RPC for catching up stale followers — manual lock management to avoid deadlock with `applyCh`
- `replicateTo()` dispatches either `AppendEntries` or `InstallSnapshot` per peer based on `nextIndex`

### Test results

```
$ cd src/raft1 && go test -race
Test (3A): initial election...                             Passed
Test (3A): election after network failure...               Passed
Test (3A): multiple elections...                           Passed
Test (3B): basic agreement...                              Passed
Test (3B): RPC byte count...                               Passed
Test (3B): test progressive failure of followers...        Passed
Test (3B): test failure of leaders...                      Passed
Test (3B): agreement after follower reconnects...          Passed
Test (3B): no agreement if too many followers disconnect...Passed
Test (3B): concurrent Start()s...                          Passed
Test (3B): rejoin of partitioned leader...                 Passed
Test (3B): leader backs up quickly over incorrect follower logs...Passed
Test (3B): RPC counts aren't too high...                   Passed
Test (3C): basic persistence...                            Passed
Test (3C): more persistence...                             Passed
Test (3C): partitioned leader and target removed then re-added...Passed
Test (3C): figure 8...                                     Passed
Test (3C): unreliable agreement...                         Passed
Test (3C): figure 8 (unreliable)...                        Passed
Test (3C): churn...                                        Passed
Test (3C): unreliable churn...                             Passed
Test (3D): snapshots basic...                              Passed
Test (3D): install snapshots (disconnect)...               Passed
Test (3D): install snapshots (disconnect) (unreliable)...  Passed
Test (3D): install snapshots (crash)...                    Passed
Test (3D): install snapshots (crash) (unreliable)...       Passed
Test (3D): crash and restart all servers...                 Passed
Test (3D): snapshot initialization after crash...          Passed
PASS
```

### Running

```bash
cd src/raft1

# Run all tests
go test -race

# Run a specific lab section
go test -run 3A -race
go test -run 3D -race -v
```
