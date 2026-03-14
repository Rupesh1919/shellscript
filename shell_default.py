import os
import sys
import shlex
import signal

# Store background jobs
jobs = {}
job_counter = 1


def clean_finished_jobs():
    """Remove completed jobs from job list."""
    finished = []

    for jid, pid in jobs.items():
        result = os.waitpid(pid, os.WNOHANG)

        if result[0] != 0:
            finished.append(jid)

    for jid in finished:
        del jobs[jid]


def execute_command(args, background=False):
    """Execute external command using fork and exec."""
    global job_counter

    pid = os.fork()

    if pid == 0:
        # Child process
        try:
            os.execvp(args[0], args)
        except FileNotFoundError:
            print("Command not found:", args[0])
            os._exit(1)

    else:
        # Parent process
        if background:
            jobs[job_counter] = pid
            print(f"[{job_counter}] started with PID {pid}")
            job_counter += 1
        else:
            os.waitpid(pid, 0)


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
            # Built-in commands
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
                    except Exception:
                        print("mkdir error")

            elif cmd == "rmdir":
                if len(args) < 2:
                    print("rmdir: missing directory name")
                else:
                    try:
                        os.rmdir(args[1])
                    except Exception:
                        print("rmdir error")

            elif cmd == "rm":
                if len(args) < 2:
                    print("rm: missing filename")
                else:
                    try:
                        os.remove(args[1])
                    except Exception:
                        print("rm error")

            elif cmd == "touch":
                if len(args) < 2:
                    print("touch: missing filename")
                else:
                    try:
                        open(args[1], "a").close()
                    except Exception:
                        print("touch error")

            elif cmd == "kill":
                if len(args) < 2:
                    print("kill: missing PID")
                else:
                    try:
                        pid = int(args[1])
                        os.kill(pid, signal.SIGTERM)
                        print(f"Process {pid} terminated")

                        # Remove from job list
                        for jid, jpid in list(jobs.items()):
                            if jpid == pid:
                                del jobs[jid]

                    except Exception:
                        print("kill error")

            # -------------------------
            # Job Control
            # -------------------------

            elif cmd == "jobs":

                if not jobs:
                    print("No background jobs")

                else:
                    for jid, pid in jobs.items():
                        result = os.waitpid(pid, os.WNOHANG)

                        if result[0] == 0:
                            print(f"[{jid}] PID:{pid} Running")
                        else:
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

                    except Exception:
                        print("fg error")

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

                    except Exception:
                        print("bg error")

            # -------------------------
            # External Commands
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
            print("Shell error:", e)


run_shell()
