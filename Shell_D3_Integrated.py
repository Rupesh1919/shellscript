import os
import sys
import shlex
import signal
import time
import heapq
from collections import deque

import threading
import random
from collections import OrderedDict
from dataclasses import dataclass

# =========================
# PART 1: BASIC SHELL
# =========================

jobs = {}
job_counter = 1


def clean_finished_jobs():
    finished = []
    for jid, pid in list(jobs.items()):
        try:
            result = os.waitpid(pid, os.WNOHANG)
            if result[0] != 0:
                finished.append(jid)
        except ChildProcessError:
            finished.append(jid)

    for jid in finished:
        del jobs[jid]


def execute_command(args, background=False):
    global job_counter

    pid = os.fork()

    if pid == 0:
        try:
            os.execvp(args[0], args)
        except FileNotFoundError:
            print(f"Command not found: {args[0]}")
            os._exit(1)
    else:
        if background:
            jobs[job_counter] = pid
            print(f"[{job_counter}] started with PID {pid}")
            job_counter += 1
        else:
            os.waitpid(pid, 0)


# =========================
# PART 2: SCHEDULER SUPPORT
# =========================

class SimProcess:
    def __init__(self, pid, name, burst_time, priority=0, arrival_order=0):
        self.pid = pid
        self.name = name
        self.burst_time = burst_time
        self.remaining_time = burst_time
        self.priority = priority
        self.arrival_order = arrival_order

        self.start_time = None
        self.finish_time = None
        self.response_time = None
        self.waiting_time = 0
        self.turnaround_time = 0

    def __repr__(self):
        return (
            f"PID={self.pid}, Name={self.name}, Burst={self.burst_time}, "
            f"Remaining={self.remaining_time}, Priority={self.priority}"
        )


class Scheduler:
    def __init__(self):
        self.reset()

    def reset(self):
        self.rr_queue = deque()
        self.priority_queue = []
        self.completed = []
        self.sim_pid_counter = 1
        self.arrival_counter = 0

    def add_rr_process(self, name, burst_time):
        p = SimProcess(
            pid=self.sim_pid_counter,
            name=name,
            burst_time=burst_time,
            arrival_order=self.arrival_counter
        )
        self.rr_queue.append(p)
        self.sim_pid_counter += 1
        self.arrival_counter += 1
        print(f"Added RR process -> {p}")

    def add_priority_process(self, name, burst_time, priority):
        p = SimProcess(
            pid=self.sim_pid_counter,
            name=name,
            burst_time=burst_time,
            priority=priority,
            arrival_order=self.arrival_counter
        )
        heapq.heappush(self.priority_queue, (priority, self.arrival_counter, p))
        self.sim_pid_counter += 1
        self.arrival_counter += 1
        print(f"Added Priority process -> {p}")

    def show_rr_queue(self):
        if not self.rr_queue:
            print("Round-Robin queue is empty.")
            return
        print("Round-Robin Queue:")
        for p in self.rr_queue:
            print(f"  {p}")

    def show_priority_queue(self):
        if not self.priority_queue:
            print("Priority queue is empty.")
            return
        print("Priority Queue:")
        for prio, order, p in sorted(self.priority_queue, key=lambda x: (x[0], x[1])):
            print(f"  PID={p.pid}, Name={p.name}, Burst={p.burst_time}, Remaining={p.remaining_time}, Priority={p.priority}")

    def run_round_robin(self, quantum):
        if not self.rr_queue:
            print("No Round-Robin processes to schedule.")
            return

        print(f"\nStarting Round-Robin Scheduling with quantum = {quantum}\n")
        current_time = 0
        completed_local = []

        while self.rr_queue:
            process = self.rr_queue.popleft()

            if process.start_time is None:
                process.start_time = current_time
                process.response_time = process.start_time

            run_time = min(quantum, process.remaining_time)

            print(
                f"Running Process PID={process.pid}, Name={process.name}, "
                f"for {run_time} unit(s). Remaining before run = {process.remaining_time}"
            )

            time.sleep(run_time)
            process.remaining_time -= run_time
            current_time += run_time

            if process.remaining_time == 0:
                process.finish_time = current_time
                process.turnaround_time = process.finish_time
                process.waiting_time = process.turnaround_time - process.burst_time
                completed_local.append(process)

                print(
                    f"Completed Process PID={process.pid}, Name={process.name}, "
                    f"Finish Time={process.finish_time}"
                )
            else:
                print(
                    f"Time slice expired for PID={process.pid}, Name={process.name}. "
                    f"Remaining Time={process.remaining_time}. Moving to back of queue."
                )
                self.rr_queue.append(process)

            print("-" * 60)

        self.completed.extend(completed_local)
        self.print_metrics(completed_local, "Round-Robin")

    def run_priority_preemptive(self):
        if not self.priority_queue:
            print("No Priority processes to schedule.")
            return

        print("\nStarting Preemptive Priority Scheduling\n")
        current_time = 0
        completed_local = []

        while self.priority_queue:
            priority, arrival_order, process = heapq.heappop(self.priority_queue)

            if process.start_time is None:
                process.start_time = current_time
                process.response_time = process.start_time

            print(
                f"Running Process PID={process.pid}, Name={process.name}, "
                f"Priority={process.priority}, Remaining={process.remaining_time}"
            )

            # Run for 1 time unit to simulate preemptive scheduling
            time.sleep(1)
            process.remaining_time -= 1
            current_time += 1

            # If during execution higher priority work was added,
            # heapq naturally handles it on the next loop iteration.
            if process.remaining_time == 0:
                process.finish_time = current_time
                process.turnaround_time = process.finish_time
                process.waiting_time = process.turnaround_time - process.burst_time
                completed_local.append(process)

                print(
                    f"Completed Process PID={process.pid}, Name={process.name}, "
                    f"Finish Time={process.finish_time}"
                )
            else:
                print(
                    f"Preempting/Re-queueing PID={process.pid}, Name={process.name}, "
                    f"Remaining={process.remaining_time}"
                )
                heapq.heappush(
                    self.priority_queue,
                    (process.priority, process.arrival_order, process)
                )

            print("-" * 60)

        self.completed.extend(completed_local)
        self.print_metrics(completed_local, "Priority-Based Scheduling")

    def print_metrics(self, processes, algorithm_name):
        if not processes:
            print(f"No completed processes for {algorithm_name}.")
            return

        print(f"\nPerformance Metrics for {algorithm_name}")
        print("=" * 72)
        print(
            f"{'PID':<8}{'Name':<15}{'Burst':<10}{'Priority':<10}"
            f"{'Waiting':<12}{'Turnaround':<14}{'Response':<10}"
        )

        total_waiting = 0
        total_turnaround = 0
        total_response = 0

        for p in processes:
            total_waiting += p.waiting_time
            total_turnaround += p.turnaround_time
            total_response += p.response_time

            print(
                f"{p.pid:<8}{p.name:<15}{p.burst_time:<10}{p.priority:<10}"
                f"{p.waiting_time:<12}{p.turnaround_time:<14}{p.response_time:<10}"
            )

        n = len(processes)
        print("=" * 72)
        print(f"Average Waiting Time   : {total_waiting / n:.2f}")
        print(f"Average Turnaround Time: {total_turnaround / n:.2f}")
        print(f"Average Response Time  : {total_response / n:.2f}")
        print()

    def show_completed(self):
        if not self.completed:
            print("No completed scheduled processes.")
            return

        print("Completed Scheduled Processes:")
        for p in self.completed:
            print(
                f"PID={p.pid}, Name={p.name}, Burst={p.burst_time}, "
                f"Priority={p.priority}, Waiting={p.waiting_time}, "
                f"Turnaround={p.turnaround_time}, Response={p.response_time}"
            )



# =========================
# DELIVERABLE 3: MEMORY MANAGEMENT (PAGING) + SYNCHRONIZATION
# =========================

@dataclass
class Frame:
    frame_id: int
    pid: int | None = None
    page: int | None = None
    loaded_at: int = 0
    last_used: int = 0

class MemoryManager:
    """
    Simple paging simulation:
    - Backing store: each process has a set of allocated page numbers.
    - Physical memory: fixed number of frames.
    - Page faults occur on access when page is not in any frame.
    - Replacement algorithms: FIFO or LRU.
    """
    def __init__(self, num_frames: int = 6, page_size: int = 4):
        self.page_size = page_size
        self.algorithm = "fifo"  # fifo | lru
        self.tick = 0
        self.init_frames(num_frames)

        # pid -> set(page_numbers)
        self.allocated_pages: dict[int, set[int]] = {}
        # pid -> page fault count
        self.page_faults: dict[int, int] = {}

    def init_frames(self, num_frames: int):
        if num_frames <= 0:
            raise ValueError("num_frames must be > 0")
        self.num_frames = num_frames
        self.frames: list[Frame] = [Frame(frame_id=i) for i in range(num_frames)]
        # FIFO order of frame_ids loaded (only for occupied frames)
        self.fifo_queue: deque[int] = deque()

    def set_algorithm(self, algo: str):
        algo = algo.lower()
        if algo not in ("fifo", "lru"):
            raise ValueError("Algorithm must be 'fifo' or 'lru'")
        self.algorithm = algo

    def alloc(self, pid: int, num_pages: int):
        if num_pages <= 0:
            raise ValueError("num_pages must be > 0")
        pages = self.allocated_pages.setdefault(pid, set())
        start = 0
        if pages:
            start = max(pages) + 1
        for p in range(start, start + num_pages):
            pages.add(p)
        self.page_faults.setdefault(pid, 0)

    def free(self, pid: int):
        # Remove allocated pages
        self.allocated_pages.pop(pid, None)
        self.page_faults.pop(pid, None)
        # Evict any frames belonging to pid
        for fr in self.frames:
            if fr.pid == pid:
                self._clear_frame(fr.frame_id)
        # Clean fifo queue
        self.fifo_queue = deque([fid for fid in self.fifo_queue if self.frames[fid].pid is not None])

    def access(self, pid: int, page: int):
        self.tick += 1
        # Validate allocation
        if pid not in self.allocated_pages or page not in self.allocated_pages[pid]:
            raise ValueError(f"PID {pid} has not allocated page {page}. Use mem_alloc first.")

        # Check if in memory
        for fr in self.frames:
            if fr.pid == pid and fr.page == page:
                fr.last_used = self.tick
                return ("hit", fr.frame_id)

        # Page fault: load it
        self.page_faults[pid] = self.page_faults.get(pid, 0) + 1
        frame_id = self._load_page(pid, page)
        return ("fault", frame_id)

    def _find_free_frame(self) -> int | None:
        for fr in self.frames:
            if fr.pid is None:
                return fr.frame_id
        return None

    def _select_victim_frame(self) -> int:
        # Memory full: choose victim by FIFO or LRU
        if self.algorithm == "fifo":
            # fifo_queue holds frame_ids in load order
            if not self.fifo_queue:
                # fallback: first frame
                return 0
            return self.fifo_queue[0]
        else:
            # LRU: smallest last_used among occupied frames
            occupied = [fr for fr in self.frames if fr.pid is not None]
            return min(occupied, key=lambda f: f.last_used).frame_id

    def _clear_frame(self, frame_id: int):
        fr = self.frames[frame_id]
        fr.pid = None
        fr.page = None
        fr.loaded_at = 0
        fr.last_used = 0

    def _load_page(self, pid: int, page: int) -> int:
        free_id = self._find_free_frame()
        if free_id is None:
            victim = self._select_victim_frame()
            # evict victim
            if self.algorithm == "fifo" and self.fifo_queue and self.fifo_queue[0] == victim:
                self.fifo_queue.popleft()
            self._clear_frame(victim)
            free_id = victim

        fr = self.frames[free_id]
        fr.pid = pid
        fr.page = page
        fr.loaded_at = self.tick
        fr.last_used = self.tick

        if self.algorithm == "fifo":
            self.fifo_queue.append(free_id)
        return free_id

    def status_lines(self) -> list[str]:
        lines = []
        lines.append(f"Paging algorithm: {self.algorithm.upper()} | Frames: {self.num_frames} | Page size: {self.page_size} units")
        lines.append("Frames:")
        for fr in self.frames:
            if fr.pid is None:
                lines.append(f"  Frame {fr.frame_id}: [FREE]")
            else:
                lines.append(f"  Frame {fr.frame_id}: PID {fr.pid} -> Page {fr.page} (loaded_at={fr.loaded_at}, last_used={fr.last_used})")

        lines.append("Per-process allocation and page faults:")
        if not self.allocated_pages:
            lines.append("  (no allocated processes)")
        else:
            for pid in sorted(self.allocated_pages.keys()):
                alloc = sorted(self.allocated_pages[pid])
                in_mem = sorted([fr.page for fr in self.frames if fr.pid == pid])
                faults = self.page_faults.get(pid, 0)
                lines.append(f"  PID {pid}: allocated_pages={alloc} | in_memory={in_mem} | page_faults={faults}")
        return lines

# Global memory manager instance (used by shell commands)
memory_manager = MemoryManager()


class ProducerConsumerDemo:
    """
    Producer-Consumer demo using semaphores + mutex to protect a shared buffer.
    Start it and observe interleavings without race conditions.
    """
    def __init__(self):
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._running = False

        self.buffer = deque()
        self.buffer_size = 5
        self.lock = threading.Lock()
        self.empty = threading.Semaphore(self.buffer_size)
        self.full = threading.Semaphore(0)

        self.produced = 0
        self.consumed = 0

    def start(self, producers: int, consumers: int, buffer_size: int, items_per_producer: int = 10):
        if self._running:
            print("Producer-Consumer demo is already running. Use pc_stop first.")
            return

        if producers <= 0 or consumers <= 0 or buffer_size <= 0 or items_per_producer <= 0:
            print("pc_start: all numeric arguments must be > 0")
            return

        self._stop.clear()
        self._running = True
        self.buffer = deque()
        self.buffer_size = buffer_size
        self.lock = threading.Lock()
        self.empty = threading.Semaphore(self.buffer_size)
        self.full = threading.Semaphore(0)
        self.produced = 0
        self.consumed = 0

        def producer(pid: int):
            for i in range(items_per_producer):
                if self._stop.is_set():
                    break
                item = f"P{pid}-{i}"
                self.empty.acquire()
                with self.lock:
                    self.buffer.append(item)
                    self.produced += 1
                    print(f"[Producer {pid}] produced {item} | buffer_len={len(self.buffer)}")
                self.full.release()
                time.sleep(random.uniform(0.02, 0.12))
            print(f"[Producer {pid}] done")

        def consumer(cid: int):
            # Consumers run until stop requested AND buffer drained
            while True:
                if self._stop.is_set():
                    # try to drain remaining items
                    with self.lock:
                        if not self.buffer:
                            break
                acquired = self.full.acquire(timeout=0.2)
                if not acquired:
                    continue
                with self.lock:
                    if self.buffer:
                        item = self.buffer.popleft()
                        self.consumed += 1
                        print(f"[Consumer {cid}] consumed {item} | buffer_len={len(self.buffer)}")
                self.empty.release()
                time.sleep(random.uniform(0.02, 0.12))
            print(f"[Consumer {cid}] done")

        # launch threads
        self._threads = []
        for p in range(1, producers + 1):
            t = threading.Thread(target=producer, args=(p,), daemon=True)
            self._threads.append(t)
            t.start()

        for c in range(1, consumers + 1):
            t = threading.Thread(target=consumer, args=(c,), daemon=True)
            self._threads.append(t)
            t.start()

        print(f"Producer-Consumer demo started: producers={producers}, consumers={consumers}, buffer_size={buffer_size}, items_per_producer={items_per_producer}")
        print("Tip: run 'pc_status' while it's running, then 'pc_stop' to end it.")

    def stop(self):
        if not self._running:
            print("Producer-Consumer demo is not running.")
            return
        self._stop.set()
        # Let consumers wake up
        for _ in range(self.buffer_size + 5):
            try:
                self.full.release()
            except ValueError:
                pass
        # give threads a moment to finish
        time.sleep(0.2)
        self._running = False
        print("Producer-Consumer demo stopped.")

    def status(self):
        running = "RUNNING" if self._running else "STOPPED"
        with self.lock:
            buf_len = len(self.buffer)
        print(f"Producer-Consumer status: {running} | buffer_len={buf_len} | produced={self.produced} | consumed={self.consumed}")


pc_demo = ProducerConsumerDemo()


def print_d3_help():
    print("\nDeliverable 3 Commands (Memory + Synchronization):")
    print("  d3_help")
    print("      Show this help menu")
    print()
    print("  mem_init <num_frames> [page_size]")
    print("      Reset physical memory frames (clears loaded pages). Optional page_size is just for display.")
    print()
    print("  paging <fifo|lru>")
    print("      Choose page replacement algorithm (FIFO or LRU)")
    print()
    print("  mem_alloc <pid> <num_pages>")
    print("      Allocate pages for a simulated PID (backing store). Pages start at 0 and increment.")
    print()
    print("  mem_access <pid> <page>")
    print("      Access a page. Shows HIT or PAGE FAULT and triggers replacement if memory is full.")
    print()
    print("  mem_free <pid>")
    print("      Deallocate a process and evict its pages from frames.")
    print()
    print("  mem_status")
    print("      Show frames, per-process allocations, and page fault counts.")
    print()
    print("  pc_start <producers> <consumers> <buffer_size> [items_per_producer]")
    print("      Start Producer-Consumer synchronization demo (threads + semaphores + mutex).")
    print()
    print("  pc_status")
    print("      Show Producer-Consumer buffer status and totals.")
    print()
    print("  pc_stop")
    print("      Stop Producer-Consumer demo.\n")


scheduler = Scheduler()


# =========================
# PART 3: HELP MENU
# =========================

def print_scheduler_help():
    print("\nScheduler Commands:")
    print("  rr_add <name> <burst_time>")
    print("      Add a process to the Round-Robin queue")
    print()
    print("  rr_show")
    print("      Show Round-Robin queue")
    print()
    print("  rr_run <quantum>")
    print("      Run Round-Robin scheduling with configurable time slice")
    print()
    print("  prio_add <name> <burst_time> <priority>")
    print("      Add a process to the Priority queue")
    print("      Lower number = higher priority")
    print()
    print("  prio_show")
    print("      Show Priority queue")
    print()
    print("  prio_run")
    print("      Run preemptive priority-based scheduling")
    print()
    print("  sched_completed")
    print("      Show completed scheduled processes and metrics history")
    print()
    print("  sched_reset")
    print("      Clear all scheduling queues and completed data")
    print()


# =========================
# PART 4: MAIN SHELL LOOP
# =========================

def run_shell():
    while True:
        clean_finished_jobs()

        try:
            command_input = input("myshell> ")

            if not command_input.strip():
                continue

            args = shlex.split(command_input)
            cmd = args[0]

            # -------------------------
            # BASIC BUILT-IN COMMANDS
            # -------------------------
            if cmd == "exit":
                print("Exiting shell...")
                sys.exit(0)

            elif cmd == "pwd":
                print(os.getcwd())

            elif cmd == "cd":
                if len(args) < 2:
                    print("cd: missing argument")
                else:
                    try:
                        os.chdir(args[1])
                    except FileNotFoundError:
                        print("cd: directory not found")

            elif cmd == "echo":
                print(" ".join(args[1:]))

            elif cmd == "clear":
                os.system("clear")

            elif cmd == "ls":
                execute_command(["ls"])

            elif cmd == "cat":
                if len(args) < 2:
                    print("cat: missing filename")
                else:
                    execute_command(args)

            elif cmd == "mkdir":
                if len(args) < 2:
                    print("mkdir: missing directory name")
                else:
                    try:
                        os.mkdir(args[1])
                    except Exception as e:
                        print(f"mkdir error: {e}")

            elif cmd == "rmdir":
                if len(args) < 2:
                    print("rmdir: missing directory name")
                else:
                    try:
                        os.rmdir(args[1])
                    except Exception as e:
                        print(f"rmdir error: {e}")

            elif cmd == "rm":
                if len(args) < 2:
                    print("rm: missing filename")
                else:
                    try:
                        os.remove(args[1])
                    except Exception as e:
                        print(f"rm error: {e}")

            elif cmd == "touch":
                if len(args) < 2:
                    print("touch: missing filename")
                else:
                    try:
                        open(args[1], "a").close()
                    except Exception as e:
                        print(f"touch error: {e}")

            elif cmd == "kill":
                if len(args) < 2:
                    print("kill: missing PID")
                else:
                    try:
                        pid = int(args[1])
                        os.kill(pid, signal.SIGTERM)
                        print(f"Process {pid} terminated")

                        for jid, jpid in list(jobs.items()):
                            if jpid == pid:
                                del jobs[jid]

                    except Exception as e:
                        print(f"kill error: {e}")

            # -------------------------
            # OS-LIKE JOB CONTROL
            # -------------------------
            elif cmd == "jobs":
                if not jobs:
                    print("No background jobs")
                else:
                    for jid, pid in jobs.items():
                        try:
                            result = os.waitpid(pid, os.WNOHANG)
                            if result[0] == 0:
                                print(f"[{jid}] PID:{pid} Running")
                            else:
                                print(f"[{jid}] PID:{pid} Finished")
                        except ChildProcessError:
                            print(f"[{jid}] PID:{pid} Finished")

            elif cmd == "fg":
                if len(args) < 2:
                    print("fg: missing job id")
                else:
                    try:
                        jid = int(args[1])

                        if jid not in jobs:
                            print("fg: No such job")
                        else:
                            pid = jobs[jid]
                            print(f"Bringing job [{jid}] to foreground")
                            os.waitpid(pid, 0)
                            del jobs[jid]
                    except Exception as e:
                        print(f"fg error: {e}")

            elif cmd == "bg":
                if len(args) < 2:
                    print("bg: missing job id")
                else:
                    try:
                        jid = int(args[1])

                        if jid not in jobs:
                            print("bg: No such job")
                        else:
                            pid = jobs[jid]
                            print(f"Job [{jid}] running in background PID {pid}")
                    except Exception as e:
                        print(f"bg error: {e}")

            # -------------------------
            # SCHEDULER COMMANDS
            # -------------------------
            elif cmd == "sched_help":
                print_scheduler_help()

            elif cmd == "sched_reset":
                scheduler.reset()
                print("Scheduler state cleared.")

            elif cmd == "rr_add":
                if len(args) != 3:
                    print("Usage: rr_add <name> <burst_time>")
                else:
                    try:
                        name = args[1]
                        burst = int(args[2])
                        if burst <= 0:
                            print("burst_time must be > 0")
                        else:
                            scheduler.add_rr_process(name, burst)
                    except ValueError:
                        print("burst_time must be an integer")

            elif cmd == "rr_show":
                scheduler.show_rr_queue()

            elif cmd == "rr_run":
                if len(args) != 2:
                    print("Usage: rr_run <quantum>")
                else:
                    try:
                        quantum = int(args[1])
                        if quantum <= 0:
                            print("quantum must be > 0")
                        else:
                            scheduler.run_round_robin(quantum)
                    except ValueError:
                        print("quantum must be an integer")

            elif cmd == "prio_add":
                if len(args) != 4:
                    print("Usage: prio_add <name> <burst_time> <priority>")
                else:
                    try:
                        name = args[1]
                        burst = int(args[2])
                        priority = int(args[3])

                        if burst <= 0:
                            print("burst_time must be > 0")
                        else:
                            scheduler.add_priority_process(name, burst, priority)

                    except ValueError:
                        print("burst_time and priority must be integers")

            elif cmd == "prio_show":
                scheduler.show_priority_queue()

            elif cmd == "prio_run":
                scheduler.run_priority_preemptive()

            elif cmd == "sched_completed":
                scheduler.show_completed()

            
            # -------------------------
            # DELIVERABLE 3 COMMANDS
            # -------------------------
            elif cmd == "d3_help":
                print_d3_help()

            elif cmd == "mem_init":
                if len(args) not in (2, 3):
                    print("Usage: mem_init <num_frames> [page_size]")
                else:
                    try:
                        num_frames = int(args[1])
                        page_size = memory_manager.page_size if len(args) == 2 else int(args[2])
                        if num_frames <= 0 or page_size <= 0:
                            print("mem_init: num_frames and page_size must be > 0")
                        else:
                            # keep allocations + faults, but clear frames
                            memory_manager.page_size = page_size
                            memory_manager.init_frames(num_frames)
                            print(f"Memory initialized: frames={num_frames}, page_size={page_size}")
                    except ValueError:
                        print("mem_init: arguments must be integers")

            elif cmd == "paging":
                if len(args) != 2:
                    print("Usage: paging <fifo|lru>")
                else:
                    try:
                        memory_manager.set_algorithm(args[1])
                        print(f"Paging algorithm set to {memory_manager.algorithm.upper()}")
                    except ValueError as e:
                        print(f"paging error: {e}")

            elif cmd == "mem_alloc":
                if len(args) != 3:
                    print("Usage: mem_alloc <pid> <num_pages>")
                else:
                    try:
                        pid = int(args[1])
                        num_pages = int(args[2])
                        memory_manager.alloc(pid, num_pages)
                        print(f"Allocated {num_pages} pages to PID {pid}")
                    except ValueError as e:
                        print(f"mem_alloc error: {e}")

            elif cmd == "mem_free":
                if len(args) != 2:
                    print("Usage: mem_free <pid>")
                else:
                    try:
                        pid = int(args[1])
                        memory_manager.free(pid)
                        print(f"Freed memory for PID {pid}")
                    except ValueError:
                        print("mem_free: pid must be an integer")

            elif cmd == "mem_access":
                if len(args) != 3:
                    print("Usage: mem_access <pid> <page>")
                else:
                    try:
                        pid = int(args[1])
                        page = int(args[2])
                        result, frame_id = memory_manager.access(pid, page)
                        if result == "hit":
                            print(f"HIT: PID {pid} page {page} is in frame {frame_id}")
                        else:
                            print(f"PAGE FAULT: loaded PID {pid} page {page} into frame {frame_id} (replacement may have occurred)")
                    except ValueError as e:
                        print(f"mem_access error: {e}")

            elif cmd == "mem_status":
                for line in memory_manager.status_lines():
                    print(line)

            elif cmd == "pc_start":
                if len(args) not in (4, 5):
                    print("Usage: pc_start <producers> <consumers> <buffer_size> [items_per_producer]")
                else:
                    try:
                        producers = int(args[1])
                        consumers = int(args[2])
                        buffer_size = int(args[3])
                        items = 10 if len(args) == 4 else int(args[4])
                        pc_demo.start(producers, consumers, buffer_size, items)
                    except ValueError:
                        print("pc_start: arguments must be integers")

            elif cmd == "pc_status":
                pc_demo.status()

            elif cmd == "pc_stop":
                pc_demo.stop()


# -------------------------
            # EXTERNAL COMMANDS
            # -------------------------
            else:
                background = False

                if args[-1] == "&":
                    background = True
                    args = args[:-1]

                execute_command(args, background)

        except KeyboardInterrupt:
            print("\nCtrl+C detected")

        except Exception as e:
            print(f"Shell error: {e}")


if __name__ == "__main__":
    print("Basic Shell with Round-Robin and Priority Scheduling")
    print("Type 'sched_help' to see scheduling commands.")
    run_shell()
