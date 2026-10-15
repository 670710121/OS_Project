# Mini Job Scheduler & Shared Resource Manager (Java)

This version completes the starter project using simple, separate classes. Comments in the Java files explain the purpose of the fields and important statements.

## Requirements

- Java 11 or newer (`Path.of` is used by the supplied workload loader).
- Run commands from the project root (`os-term-project-starter` after extracting the ZIP).

## Compile

```bash
javac src/*.java
```

## Run

```bash
java -cp src Main workloads/jobs_standard.csv priority 3 1 2
java -cp src Main workloads/jobs_standard.csv fcfs 3 1 2
```

Arguments, in order:

1. CSV workload path
2. `fcfs` or `priority`
3. Worker thread count
4. Printer permit count
5. Database permit count

All five arguments are required. Worker and permit counts must be at least 1.

## Main classes

- `Main`: validates arguments, loads the CSV, creates all shared objects, starts and joins threads, and prints the summary.
- `JobGenerator`: releases jobs at `arrivalMs` times into the arrival `BlockingQueue`.
- `Scheduler`: a real thread that takes jobs from the arrival queue and forwards them to the ready queue.
- `ReadyQueue`: common interface so Worker does not depend on a specific policy.
- `FCFSQueue`: selects the job that entered the ready queue first.
- `PriorityQueueImpl`: selects the smallest priority number first; ties are resolved by original CSV sequence.
- `Worker`: runs the main work first, then waits for and uses the requested resource, then records completion.
- `ResourceManager`: shared fair Semaphores control how many jobs can use the Printer and Database simultaneously.
- `Statistics`: thread-safe running/completed counters and average metrics.
- `Monitor`: prints a system snapshot about every 1,000 ms.
- `WorkloadLoader` and `ProjectLogger`: supplied helper classes retained from the starter.

## Shutdown design

The Generator sends a special stop marker through the arrival queue after releasing all real jobs. Scheduler consumes that marker only after all earlier arrivals, then shuts down the ready queue. Workers drain the ready queue and exit when it is empty and shut down. Main joins the Generator, Scheduler, and every Worker, then interrupts and joins Monitor. No arbitrary sleep is used to guess when threads have finished.

Workers retry a resource acquire if they are interrupted while waiting, and the resource-use section uses `finally` to return a permit after successful acquisition. Simulated sleep also completes its remaining duration after an interruption so a job is not silently abandoned.

## Metrics

- Waiting Time = worker start time − actual arrival time.
- Turnaround Time = completion time − actual arrival time.
- Resource Wait Time = time from beginning Semaphore wait until successful acquire; the average includes only jobs that request a resource.
- Throughput = completed jobs / makespan in seconds.

Sleep and thread scheduling can add small real-time overhead, so timing values may vary slightly between runs. Use logs as well as the summary when comparing policies.

## Save logs

```bash
java -cp src Main workloads/jobs_standard.csv priority 3 1 2 > priority.log
java -cp src Main workloads/jobs_standard.csv fcfs 3 1 2 > fcfs.log
```

## Suggested tests from the assignment

```bash
java -cp src Main workloads/jobs_single.csv priority 1 1 2
java -cp src Main workloads/jobs_standard.csv fcfs 3 1 2
java -cp src Main workloads/jobs_standard.csv priority 3 1 2
java -cp src Main workloads/jobs_printer.csv priority 3 1 2
java -cp src Main workloads/jobs_printer.csv priority 3 2 2
java -cp src Main workloads/jobs_db.csv priority 3 1 2
java -cp src Main workloads/jobs_same_priority.csv priority 3 1 2
```

## Important note

This is an educational simulation. Resource use and work are represented by `Thread.sleep`, not real CPU work, printers, or database connections. Review the source and test each case before presenting it; being able to explain and modify the code is part of the assignment.
