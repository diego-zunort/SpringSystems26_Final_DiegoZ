# Simulation Runner (Rust)

## How to run

From the project root:

For Balanced run:

```bash
cargo run -- 70-30
```

For Stress run:

```bash
cargo run -- 80-20
```
The default run is Balanced

## What the program does

- Generates `1000` tasks at a fixed `20 ms` arrival interval.
- Task mix depends on the workload arg:
  - `70-30`: 70% IO tasks / 30% CPU tasks
  - `80-20`: 80% IO tasks / 20% CPU tasks
- Each task runs for `200 ms` using `thread::sleep` as a way to simulate the process being completed
- Depending on the task kind, the following resource consumption is set:
  - CPU task: `35%`
  - IO task: `10%`

## Scheduling policies

The program runs two simulations back-to-back and compares them:

- `FIFO`: prioritizes CPU queue first, then IO queue.
- `Optimized`: prioritizes IO queue first, then CPU queue.

CPU caps at 100% and will not dispatch a task if the resource consumption of said task makes the load surpass the cap.

## Output

For each simulation it prints a metrics summary:
- total tasks completed (CPU/IO breakdown)
- total runtime (makespan)
- average wait time and turnaround time
- average/peak CPU usage (from monitor samples every 10 ms)
- average active workers

## Example runs:

```bash
Workload   : 70% IO / 30% CPU  |  1000 tasks, 20 ms intervals
Workers    : 8
Task times : CPU = 35% load, IO = 10% load, both run 200 ms
CPU cap    : 100%  (manager blocks dispatch if cap would be exceeded)

Running Simulation 1 — FIFO...

Running Simulation 2 — Optimized...

--- Simulation 1 — FIFO ---
Total tasks completed : 1000  (CPU 287 / IO 713)
Total runtime         : 47513 ms
Avg wait time         : 20886 ms
Avg turnaround time   : 21086 ms
Avg CPU usage         : 74%  (peak 100%)
Avg active workers    : 4 / 8

--- Simulation 2 — Optimized ---
Total tasks completed : 1000  (CPU 287 / IO 713)
Total runtime         : 45705 ms
Avg wait time         : 5607 ms
Avg turnaround time   : 5807 ms
Avg CPU usage         : 76%  (peak 100%)
Avg active workers    : 4 / 8

=== Comparison ===
Runtime    : FIFO 47513 ms  vs  Optimized 45705 ms  (1.04x speedup)
Avg CPU    : FIFO 74%  vs  Optimized 76%
```

```bash
Workload   : 80% IO / 20% CPU  |  1000 tasks, 20 ms intervals
Workers    : 8
Task times : CPU = 35% load, IO = 10% load, both run 200 ms
CPU cap    : 100%  (manager blocks dispatch if cap would be exceeded)

Running Simulation 1 — FIFO...

Running Simulation 2 — Optimized...

--- Simulation 1 — FIFO ---
Total tasks completed : 1000  (CPU 199 / IO 801)
Total runtime         : 41128 ms
Avg wait time         : 15815 ms
Avg turnaround time   : 16016 ms
Avg CPU usage         : 75%  (peak 100%)
Avg active workers    : 5 / 8

--- Simulation 2 — Optimized ---
Total tasks completed : 1000  (CPU 199 / IO 801)
Total runtime         : 40138 ms
Avg wait time         : 4133 ms
Avg turnaround time   : 4334 ms
Avg CPU usage         : 75%  (peak 100%)
Avg active workers    : 5 / 8

=== Comparison ===
Runtime    : FIFO 41128 ms  vs  Optimized 40138 ms  (1.02x speedup)
Avg CPU    : FIFO 75%  vs  Optimized 75%
```