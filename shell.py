import os
import subprocess
import shlex
import signal

# Dictionary to store background jobs
jobs = {}
job_counter = 1


def clean_jobs():
    """Remove completed jobs from the list."""
    finished = []
    for jid, proc in jobs.items():
        if proc.poll() is not None:
            finished.append(jid)

    for jid in finished:
        del jobs[jid]


def run_shell():
    global job_counter

    while True:
        clean_jobs()

        try:
            cmd_input = input("myshell> ")

            if not cmd_input.strip():
                continue

            args = shlex.split(cmd_input)
            command = args[0]

            # -------------------------
            # BUILT-IN COMMANDS
            # -------------------------

            if command == "exit":
                print("Exiting shell...")
                break

            elif command == "pwd":
                print(os.getcwd())

            elif command == "cd":
                if len(args) < 2:
                    print("cd: missing argument")
                else:
                    try:
                        os.chdir(args[1])
                    except Exception as e:
                        print("cd error:", e)

            elif command == "echo":
                print(" ".join(args[1:]))

            elif command == "clear":
                os.system("clear")

            elif command == "ls":
                os.system("ls")

            elif command == "cat":
                if len(args) < 2:
                    print("cat: missing filename")
                else:
                    try:
                        with open(args[1], "r") as f:
                            print(f.read())
                    except Exception as e:
                        print("cat error:", e)

            elif command == "mkdir":
                if len(args) < 2:
                    print("mkdir: missing directory name")
                else:
                    try:
                        os.mkdir(args[1])
                    except Exception as e:
                        print("mkdir error:", e)

            elif command == "rmdir":
                if len(args) < 2:
                    print("rmdir: missing directory name")
                else:
                    try:
                        os.rmdir(args[1])
                    except Exception as e:
                        print("rmdir error:", e)

            elif command == "rm":
                if len(args) < 2:
                    print("rm: missing filename")
                else:
                    try:
                        os.remove(args[1])
                    except Exception as e:
                        print("rm error:", e)

            elif command == "touch":
                if len(args) < 2:
                    print("touch: missing filename")
                else:
                    try:
                        open(args[1], "a").close()
                    except Exception as e:
                        print("touch error:", e)

            elif command == "kill":
                if len(args) < 2:
                    print("kill: missing PID")
                else:
                    try:
                        pid = int(args[1])
                        os.kill(pid, signal.SIGTERM)
                    except Exception as e:
                        print("kill error:", e)

            # -------------------------
            # JOB CONTROL
            # -------------------------

            elif command == "jobs":
                if not jobs:
                    print("No background jobs.")
                else:
                    for jid, proc in jobs.items():
                        status = "Running" if proc.poll() is None else "Finished"
                        print(f"[{jid}] PID:{proc.pid} {status}")

            elif command == "fg":
                if len(args) < 2:
                    print("fg: missing job id")
                else:
                    jid = int(args[1])

                    if jid not in jobs:
                        print("fg: No such job")
                    else:
                        proc = jobs[jid]
                        print(f"Bringing job [{jid}] to foreground")
                        proc.wait()
                        del jobs[jid]

            elif command == "bg":
                if len(args) < 2:
                    print("bg: missing job id")
                else:
                    jid = int(args[1])

                    if jid not in jobs:
                        print("bg: No such job")
                    else:
                        proc = jobs[jid]
                        print(f"Job [{jid}] running in background (PID {proc.pid})")

            # -------------------------
            # EXTERNAL COMMANDS
            # -------------------------

            else:

                # Background execution
                if args[-1] == "&":
                    args = args[:-1]

                    proc = subprocess.Popen(args)

                    jobs[job_counter] = proc

                    print(f"[{job_counter}] started with PID {proc.pid}")

                    job_counter += 1

                # Foreground execution
                else:
                    proc = subprocess.Popen(args)
                    proc.wait()

        except KeyboardInterrupt:
            print("\nKeyboard interrupt received.")

        except Exception as e:
            print("Shell error:", e)


run_shell()