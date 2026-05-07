
// Imports ==================================================================

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};


// Constants =================================================================

const TASK_DURATION_MS: u64 = 200;
const CPU_PERCENT: u32 = 35;
const IO_PERCENT: u32 = 10;
const SEED: u64 = 1;
const NUM_WORKERS: usize = 8;


// Enums ======================================================================

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TaskKind{
    Io,
    Cpu,
}

#[derive(Debug, Clone, Copy)]
enum Policy{
    Fifo,
    Optimized,
}


// Core Types =======================================================

pub struct CompletionReport {
    #[allow(dead_code)]
    pub task_id: u64,
    pub worker_id: usize,
    pub kind: TaskKind,
    pub arrival_time: Instant,
    pub start_time: Instant,
    pub end_time: Instant,
}

#[derive(Debug, Clone, Copy)]
struct WorkloadConfig {
    num_tasks: u64,
    seed: u64,
    io_fraction: f64,
    arrival_interval_ms: u64,
}

pub struct MonitorSample {
    pub cpu_percent: u32,
    pub active_workers: usize,
}

pub struct Metrics {
    pub reports: Vec<CompletionReport>,
    pub samples: Vec<MonitorSample>,
    run_start: Instant,
    run_end: Option<Instant>,
}



// Metrics ==========================================================

impl Metrics {
    pub fn new() -> Self {
        Self {
            reports: Vec::new(),
            samples: Vec::new(),
            run_start: Instant::now(),
            run_end: None,
        }
    }

    pub fn record(&mut self, report: CompletionReport) {
        self.reports.push(report);
    }

    pub fn add_sample(&mut self, sample: MonitorSample) {
        self.samples.push(sample);
    }

    pub fn finalize(&mut self) {
        self.run_end = Some(Instant::now());
    }

    pub fn print_summary(&self, label: &str) {
        let total = self.reports.len();
        let makespan_ms = self
            .run_end
            .unwrap_or_else(Instant::now)
            .duration_since(self.run_start)
            .as_millis() as u64;

        let cpu_count = self
            .reports
            .iter()
            .filter(|r| r.kind == TaskKind::Cpu)
            .count();
        let io_count = self
            .reports
            .iter()
            .filter(|r| r.kind == TaskKind::Io)
            .count();

        let avg_cpu = if self.samples.is_empty() {
            0
        } else {
            self.samples
                .iter()
                .map(|s| s.cpu_percent as u64)
                .sum::<u64>()
                / self.samples.len() as u64
        };
        let avg_workers = if self.samples.is_empty() {
            0
        } else {
            self.samples
                .iter()
                .map(|s| s.active_workers as u64)
                .sum::<u64>()
                / self.samples.len() as u64
        };
        let peak_cpu = self.samples.iter().map(|s| s.cpu_percent).max().unwrap_or(0);

        let avg_wait_ms = if total == 0 {
            0
        } else {
            self.reports
                .iter()
                .map(|r| r.start_time.duration_since(r.arrival_time).as_millis() as u64)
                .sum::<u64>()
                / total as u64
        };
        let avg_turnaround_ms = if total == 0 {
            0
        } else {
            self.reports
                .iter()
                .map(|r| r.end_time.duration_since(r.arrival_time).as_millis() as u64)
                .sum::<u64>()
                / total as u64
        };

        println!("--- {label} ---");
        println!("Total tasks completed : {total}  (CPU {cpu_count} / IO {io_count})");
        println!("Total runtime         : {makespan_ms} ms");
        println!("Avg wait time         : {avg_wait_ms} ms");
        println!("Avg turnaround time   : {avg_turnaround_ms} ms");
        println!("Avg CPU usage         : {avg_cpu}%  (peak {peak_cpu}%)");
        println!("Avg active workers    : {avg_workers} / 8");
    }

    pub fn total_runtime_ms(&self) -> u64 {
        self.run_end
            .unwrap_or_else(Instant::now)
            .duration_since(self.run_start)
            .as_millis() as u64
    }

    pub fn avg_cpu(&self) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples
            .iter()
            .map(|s| s.cpu_percent as u64)
            .sum::<u64>()
            / self.samples.len() as u64
    }
}

// Display / Helpers ====================================================

#[derive(Debug, Clone)]
struct Task{
    id: u64,
    arrival_time: Instant,
    kind: TaskKind,
    cpu_percent: u32,
}

//Implementation =========================================================

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Policy::Fifo => write!(f, "FIFO"),
            Policy::Optimized => write!(f, "Optimized"),
        }
    }
}

impl WorkloadConfig {
    fn standard(seed: u64) -> Self {
        Self {
            num_tasks: 1000,
            seed,
            io_fraction: 0.70,
            arrival_interval_ms: 20,
        }
    }

    fn heavy_io(seed: u64) -> Self {
        Self {
            num_tasks: 1000,
            seed: seed + 1,
            io_fraction: 0.80,
            arrival_interval_ms: 20,
        }
    }
}


// Task Constructors ======================================================

impl Task {
    pub fn new_cpu(id: u64) -> Self {
        Self {
            id,
            arrival_time: Instant::now(),
            kind: TaskKind::Cpu,
            cpu_percent: CPU_PERCENT,
        }
    }

    pub fn new_io(id: u64) -> Self {
        Self {
            id,
            arrival_time: Instant::now(),
            kind: TaskKind::Io,
            cpu_percent: IO_PERCENT,
        }
    }
}


// Generator ================================================================


fn generator_run(tx: Sender<Task>, config: WorkloadConfig) {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let interval = Duration::from_millis(config.arrival_interval_ms);

    for id in 0..config.num_tasks {
        thread::sleep(interval);
        let task = if rng.r#gen::<f64>() < config.io_fraction {
            Task::new_io(id)
        } else {
            Task::new_cpu(id)
        };
        if tx.send(task).is_err() {
            break;
        }
    }
}


// Worker ===============================================================================

fn worker_run(id: usize, rx: Receiver<Option<Task>>, comp_tx: Sender<CompletionReport>) {
    loop {
        match rx.recv() {
            Ok(Some(task)) => {
                let start_time = Instant::now();
                thread::sleep(Duration::from_millis(TASK_DURATION_MS));
                let end_time = Instant::now();

                let report = CompletionReport {
                    task_id: task.id,
                    worker_id: id,
                    kind: task.kind,
                    arrival_time: task.arrival_time,
                    start_time,
                    end_time,
                };
                if comp_tx.send(report).is_err() {
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
}


// Simulation ==========================================================================


fn run_simulation(config: WorkloadConfig, policy: Policy, label: &str) -> Metrics {
    const MONITOR_INTERVAL_MS: u64 = 10;
    println!("Running {label}...");

    let cpu_percent = Arc::new(AtomicU32::new(0));
    let busy_workers = Arc::new(AtomicUsize::new(0));
    let stop_monitor = Arc::new(AtomicBool::new(false));

    let metrics = Arc::new(Mutex::new(Metrics::new()));

    let (task_tx, task_rx) = mpsc::channel::<Task>();
    let (done_tx, done_rx) = mpsc::channel::<CompletionReport>();

    let mut worker_txs = Vec::with_capacity(NUM_WORKERS);
    let mut worker_handles = Vec::with_capacity(NUM_WORKERS);

    for worker_id in 0..NUM_WORKERS {
        let (wtx, wrx) = mpsc::channel::<Option<Task>>();
        worker_txs.push(wtx);
        let done_tx_c = done_tx.clone();
        worker_handles.push(thread::spawn(move || worker_run(worker_id, wrx, done_tx_c)));
    }
    drop(done_tx);

    let manager_handle = {
        let metrics_m = Arc::clone(&metrics);
        let cpu_percent_m = Arc::clone(&cpu_percent);
        let busy_workers_m = Arc::clone(&busy_workers);
        thread::spawn(move || {
            let mut cpu_queue: Vec<Task> = Vec::new();
            let mut io_queue: Vec<Task> = Vec::new();

            let mut next_worker = 0usize;
            let mut worker_free = vec![true; worker_txs.len()];
            let mut completed = 0usize;
            let expected = config.num_tasks as usize;
            let mut generator_done = false;
            let _start = Instant::now();

            loop {
                while let Ok(task) = task_rx.try_recv() {
                    match task.kind {
                        TaskKind::Cpu => cpu_queue.push(task),
                        TaskKind::Io => io_queue.push(task),
                    }
                }

                while let Ok(report) = done_rx.try_recv() {
                    completed += 1;
                    if report.worker_id < worker_free.len() {
                        worker_free[report.worker_id] = true;
                    }
                    busy_workers_m.fetch_sub(1, Ordering::Relaxed);
                    let kind = report.kind;
                    if let Ok(mut m) = metrics_m.lock() {
                        m.record(report);
                    }

                    let cpu_cost_u32 = match kind {
                        TaskKind::Cpu => CPU_PERCENT,
                        TaskKind::Io => IO_PERCENT,
                    };
                    cpu_percent_m.fetch_sub(cpu_cost_u32, Ordering::Relaxed);
                    if completed >= expected {
                        break;
                    }
                }

                if completed >= expected {
                    break;
                }

                for _ in 0..worker_txs.len() {
                    let worker_id = next_worker;
                    next_worker = (next_worker + 1) % worker_txs.len();
                    if !worker_free[worker_id] {
                        continue;
                    }

                    let maybe_task = match policy {
                        Policy::Fifo => {
                            if !cpu_queue.is_empty() {
                                Some(cpu_queue.remove(0))
                            } else if !io_queue.is_empty() {
                                Some(io_queue.remove(0))
                            } else {
                                None
                            }
                        }
                        Policy::Optimized => {
                            if !io_queue.is_empty() {
                                Some(io_queue.remove(0))
                            } else if !cpu_queue.is_empty() {
                                Some(cpu_queue.remove(0))
                            } else {
                                None
                            }
                        }
                    };

                    if let Some(task) = maybe_task {
                        let task_cpu = task.cpu_percent;
                        let current_cpu = cpu_percent_m.load(Ordering::Relaxed);
                        if current_cpu.saturating_add(task_cpu) > 100 {
                            match task.kind {
                                TaskKind::Cpu => cpu_queue.insert(0, task),
                                TaskKind::Io => io_queue.insert(0, task),
                            }
                            continue;
                        }

                        cpu_percent_m.fetch_add(task_cpu, Ordering::Relaxed);
                        worker_free[worker_id] = false;
                        busy_workers_m.fetch_add(1, Ordering::Relaxed);
                        let _ = worker_txs[worker_id].send(Some(task));
                    }
                }

                match task_rx.recv_timeout(Duration::from_millis(1)) {
                    Ok(task) => match task.kind {
                        TaskKind::Cpu => cpu_queue.push(task),
                        TaskKind::Io => io_queue.push(task),
                    },
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        generator_done = true;
                    }
                }

                if generator_done
                    && cpu_queue.is_empty()
                    && io_queue.is_empty()
                    && worker_free.iter().all(|v| *v)
                {
                    break;
                }
            }

            for wtx in worker_txs {
                let _ = wtx.send(None);
            }
        })
    };

    let generator_handle = thread::spawn({
        let task_tx_g = task_tx.clone();
        let cfg = config;
        move || generator_run(task_tx_g, cfg)
    });

    let monitor_handle = {
        let cpu_percent_m = Arc::clone(&cpu_percent);
        let busy_workers_m = Arc::clone(&busy_workers);
        let metrics_m = Arc::clone(&metrics);
        let stop_m = Arc::clone(&stop_monitor);
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(MONITOR_INTERVAL_MS));
            if stop_m.load(Ordering::Relaxed) {
                break;
            }
            let sample = MonitorSample {
                cpu_percent: cpu_percent_m.load(Ordering::Relaxed),
                active_workers: busy_workers_m.load(Ordering::Relaxed),
            };
            if let Ok(mut m) = metrics_m.lock() {
                m.add_sample(sample);
            }
        })
    };

    let _ = generator_handle.join();
    drop(task_tx);
    let _ = manager_handle.join();

    stop_monitor.store(true, Ordering::Relaxed);
    let _ = monitor_handle.join();

    for h in worker_handles {
        let _ = h.join();
    }

    let out = {
        let mut guard = metrics.lock().unwrap();
        guard.finalize();
        std::mem::replace(&mut *guard, Metrics::new())
    };
    out
}

// Main ====================================================================================

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let workload_arg = args.get(1).map(|s| s.as_str()).unwrap_or("70-30");

    let (config_a, config_b, workload_label) = match workload_arg {
        "80-20" => (
            WorkloadConfig::heavy_io(SEED),
            WorkloadConfig::heavy_io(SEED),
            "80% IO / 20% CPU",
        ),
        _ => (
            WorkloadConfig::standard(SEED),
            WorkloadConfig::standard(SEED),
            "70% IO / 30% CPU",
        ),
    };

    println!("Workload   : {workload_label}  |  1000 tasks, 20 ms intervals");
    println!("Workers    : {NUM_WORKERS}");
    println!("Task times : CPU = 35% load, IO = 10% load, both run 200 ms");
    println!("CPU cap    : 100%  (manager blocks dispatch if cap would be exceeded)");
    println!();

    let result_fifo = run_simulation(config_a, Policy::Fifo, "Simulation 1 — FIFO");
    println!();
    let result_opt = run_simulation(config_b, Policy::Optimized, "Simulation 2 — Optimized");
    println!();

    result_fifo.print_summary("Simulation 1 — FIFO");
    println!();
    result_opt.print_summary("Simulation 2 — Optimized");
    println!();

    let rt_fifo = result_fifo.total_runtime_ms();
    let rt_opt = result_opt.total_runtime_ms();
    let speedup = rt_fifo as f64 / rt_opt as f64;
    println!("=== Comparison ===");
    println!(
        "Runtime    : FIFO {rt_fifo} ms  vs  Optimized {rt_opt} ms  ({speedup:.2}x speedup)"
    );
    println!(
        "Avg CPU    : FIFO {}%  vs  Optimized {}%",
        result_fifo.avg_cpu(),
        result_opt.avg_cpu()
    );
}
