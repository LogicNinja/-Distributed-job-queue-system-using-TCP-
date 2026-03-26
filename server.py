import socket
import threading
import ssl
import time
from queue import Queue

HOST = "127.0.0.1"
PORT = 5000

job_queue = Queue()
job_counter = 1
counter_lock = threading.Lock()

in_progress = {}
in_progress_lock = threading.Lock()

completed_jobs = set()
completed_jobs_lock = threading.Lock()

results = {}
results_lock = threading.Lock()

client_events = {}
client_events_lock = threading.Lock()

dispatch_lock = threading.Lock()

job_submit_times = {}
job_dispatch_times = {}
job_complete_times = {}
metrics_lock = threading.Lock()

WORKER_TIMEOUT = 60

VALID_OPERATIONS = {"ADD", "SUB", "MUL", "DIV"}


def validate_job(job):
    parts = job.strip().split()
    if len(parts) != 3:
        return False, "ERROR invalid format. Use: <OP> <num> <num>"
    op, a, b = parts
    if op not in VALID_OPERATIONS:
        return False, f"ERROR unknown operation '{op}'. Valid: ADD SUB MUL DIV"
    try:
        int(a)
        int(b)
    except ValueError:
        return False, "ERROR operands must be integers"
    return True, None


def requeue_stale_jobs():
    while True:
        time.sleep(5)
        now = time.time()
        with in_progress_lock:
            stale = [
                jid for jid, (job, ts) in in_progress.items()
                if now - ts > WORKER_TIMEOUT
            ]
            for jid in stale:
                job, _ = in_progress.pop(jid)
                job_queue.put((jid, job))
                print(f"Re-queued stale job {jid}")


def handle_client(conn, addr):
    global job_counter
    print("Connected:", addr)

    try:
        conn.settimeout(30)
        try:
            initial = conn.recv(1024).decode().strip()
        except socket.timeout:
            print(f"Client {addr} timed out during handshake")
            return
        except UnicodeDecodeError:
            print(f"Client {addr} sent malformed data")
            try:
                conn.send("ERROR malformed request".encode())
            except Exception:
                pass
            return

        if not initial:
            print(f"Client {addr} disconnected before sending data")
            return

        if initial.startswith("WORKER"):
            handle_worker(conn)

        elif initial.startswith("SUBMIT"):
            job = initial.replace("SUBMIT ", "", 1).strip()

            if not job:
                conn.send("ERROR empty job".encode())
                return

            valid, error_msg = validate_job(job)
            if not valid:
                conn.send(error_msg.encode())
                print(f"Invalid job from {addr}: {job} -> {error_msg}")
                return

            with counter_lock:
                jid = job_counter
                job_counter += 1

            with metrics_lock:
                job_submit_times[jid] = time.time()

            job_queue.put((jid, job))

            event = threading.Event()
            with client_events_lock:
                client_events[jid] = event

            try:
                conn.send(f"JOB_ACCEPTED {jid}".encode())
            except Exception:
                print(f"Client {addr} disconnected after job {jid} was queued — job stays in queue")
                with client_events_lock:
                    client_events.pop(jid, None)
                return

            print(f"Job accepted: {jid} -> {job}")

            conn.settimeout(None)
            event.wait(timeout=300)

            with results_lock:
                result = results.pop(jid, None)

            try:
                if result is not None:
                    conn.send(f"RESULT {jid} {result}".encode())
                else:
                    conn.send(f"RESULT {jid} TIMEOUT".encode())
            except Exception:
                print(f"Client {addr} disconnected before receiving result for job {jid}")

            with client_events_lock:
                client_events.pop(jid, None)

        elif initial.startswith("QUERY"):
            parts = initial.split()
            if len(parts) == 2:
                try:
                    jid = int(parts[1])
                    with results_lock:
                        result = results.get(jid)
                    if result is not None:
                        conn.send(f"RESULT {jid} {result}".encode())
                    else:
                        conn.send(f"PENDING {jid}".encode())
                except ValueError:
                    conn.send("ERROR invalid job id".encode())
            else:
                conn.send("ERROR usage: QUERY <job_id>".encode())

        else:
            conn.send("ERROR unknown command".encode())
            print(f"Unknown command from {addr}: {initial[:50]}")

    except ConnectionResetError:
        print(f"Client {addr} forcibly disconnected")
    except Exception as e:
        print(f"Error with {addr}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        print("Disconnected:", addr)


def handle_worker(conn):
    print("Worker session started")
    current_job = None
    try:
        while True:
            try:
                conn.settimeout(30)
                data = conn.recv(1024).decode().strip()
            except socket.timeout:
                continue
            except UnicodeDecodeError:
                print("Worker sent malformed data, skipping")
                continue

            if not data:
                break

            if data == "GET_JOB":
                current_job = None
                with dispatch_lock:
                    try:
                        jid, job = job_queue.get(timeout=1)
                        with in_progress_lock:
                            in_progress[jid] = (job, time.time())
                        with metrics_lock:
                            job_dispatch_times[jid] = time.time()
                        current_job = jid
                        conn.send(f"JOB {jid} {job}".encode())
                        print(f"Dispatched job {jid} to worker")
                    except Exception:
                        conn.send("NO_JOB".encode())

            elif data.startswith("DONE"):
                parts = data.split()
                if len(parts) >= 3:
                    try:
                        jid = int(parts[1])
                        result = parts[2]
                        current_job = None

                        with completed_jobs_lock:
                            if jid in completed_jobs:
                                conn.send(f"ACK {jid}".encode())
                                continue
                            completed_jobs.add(jid)

                        with in_progress_lock:
                            in_progress.pop(jid, None)

                        with metrics_lock:
                            job_complete_times[jid] = time.time()

                        with results_lock:
                            results[jid] = result

                        with client_events_lock:
                            event = client_events.get(jid)
                        if event:
                            event.set()

                        conn.send(f"ACK {jid}".encode())
                        print(f"Job {jid} completed with result {result}")
                    except ValueError:
                        conn.send("ERROR invalid job id".encode())
                else:
                    conn.send("ERROR malformed DONE message".encode())
            else:
                conn.send("ERROR unknown command".encode())

    except ConnectionResetError:
        print(f"Worker forcibly disconnected")
    except Exception as e:
        print(f"Worker error: {e}")
    finally:
        if current_job is not None:
            with in_progress_lock:
                entry = in_progress.pop(current_job, None)
            if entry:
                job, _ = entry
                job_queue.put((current_job, job))
                print(f"Worker crashed mid-job — re-queued job {current_job}")


def start_server():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile="cert.pem", keyfile="key.pem")

    requeue_thread = threading.Thread(target=requeue_stale_jobs, daemon=True)
    requeue_thread.start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()

    print(f"Secure Job Server running on port {PORT}")

    while True:
        conn, addr = server.accept()
        try:
            secure_conn = context.wrap_socket(conn, server_side=True)
        except ssl.SSLError as e:
            print(f"SSL handshake failed with {addr}: {e}")
            try:
                conn.close()
            except Exception:
                pass
            continue
        except Exception as e:
            print(f"Connection error with {addr}: {e}")
            try:
                conn.close()
            except Exception:
                pass
            continue
        thread = threading.Thread(target=handle_client, args=(secure_conn, addr), daemon=True)
        thread.start()


start_server()