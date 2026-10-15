# Design summary

## Scheduling design
`ReadyQueue` is an interface. `FCFSQueue` and `PriorityQueueImpl` implement it independently. Workers call only `take()`, so the worker code is the same for both scheduling policies.

## Thread communication
`JobGenerator` writes to a `LinkedBlockingQueue<Job>`. `Scheduler` takes from that queue and inserts each real job into the ready queue. A dedicated stop marker tells Scheduler that all arrivals have been sent. Scheduler then shuts down the ready queue; workers drain it before exiting.

## Resource safety
`ResourceManager` owns fair Semaphores. `Worker` marks whether acquisition succeeded and releases the resource in a `finally` block. If interrupted while waiting for a permit, it retries instead of abandoning the job.

## Metrics
`Job` stores actual arrival, start, completion, and resource-wait timestamps using the shared `ProjectLogger` clock. `Statistics` safely records completions and reports the required averages and throughput.

## Tests performed in this workspace
- Compiled all Java source files successfully.
- Ran `jobs_single.csv`.
- Ran `jobs_same_priority.csv` with Priority.
- Ran `jobs_db.csv` with Priority and two Database permits.
- Ran `jobs_standard.csv` with FCFS and Priority.
- Ran `jobs_printer.csv` with one and two Printer permits.
- Each of these runs exited normally and reported all jobs completed.

These are basic runtime checks, not a formal proof of correctness on every possible input or interruption scenario.
