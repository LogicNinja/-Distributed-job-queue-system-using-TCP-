import socket
import ssl
import time

HOST = "127.0.0.1"
PORT = 5000

context = ssl._create_unverified_context()

MAX_RETRIES = 5


def compute(operation, a, b):
    if operation == "ADD":
        return a + b
    elif operation == "SUB":
        return a - b
    elif operation == "MUL":
        return a * b
    elif operation == "DIV":
        if b == 0:
            return "ERROR_DIV_ZERO"
        return round(a / b, 4)
    else:
        return "UNKNOWN_OP"


def connect():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            worker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            secure_worker = context.wrap_socket(worker)
            secure_worker.connect((HOST, PORT))
            print("Connected to server")
            return secure_worker
        except ssl.SSLError as e:
            print(f"SSL error during connect: {e}. Retrying...")
        except ConnectionRefusedError:
            print(f"Server not available. Retrying ({retries + 1}/{MAX_RETRIES})...")
        except Exception as e:
            print(f"Connection error: {e}. Retrying...")
        retries += 1
        time.sleep(3)
    print("Max retries reached. Exiting.")
    return None


def parse_job(data):
    try:
        parts = data.split()
        if len(parts) != 5:
            return None, None, None, None
        jid = parts[1]
        operation = parts[2]
        a = int(parts[3])
        b = int(parts[4])
        return jid, operation, a, b
    except (ValueError, IndexError):
        return None, None, None, None


def run():
    while True:
        conn = connect()
        if conn is None:
            break

        try:
            conn.settimeout(30)
            conn.send("WORKER".encode())
            conn.send("GET_JOB".encode())

            while True:
                try:
                    data = conn.recv(1024).decode().strip()
                except socket.timeout:
                    conn.send("GET_JOB".encode())
                    continue
                except UnicodeDecodeError:
                    print("Received malformed data from server, skipping")
                    conn.send("GET_JOB".encode())
                    continue

                if not data:
                    print("Server closed connection")
                    break

                if data == "NO_JOB":
                    time.sleep(1)
                    conn.send("GET_JOB".encode())

                elif data.startswith("JOB"):
                    jid, operation, a, b = parse_job(data)

                    if jid is None:
                        print(f"Malformed job received: {data}")
                        conn.send("GET_JOB".encode())
                        continue

                    try:
                        result = compute(operation, a, b)
                        print(f"Completed Job {jid}: {operation} {a} {b} = {result}")
                        conn.send(f"DONE {jid} {result}".encode())
                    except Exception as e:
                        print(f"Computation error for job {jid}: {e}")
                        conn.send(f"DONE {jid} ERROR_COMPUTE".encode())

                elif data.startswith("ACK"):
                    conn.send("GET_JOB".encode())

                elif data.startswith("ERROR"):
                    print(f"Server error: {data}")
                    conn.send("GET_JOB".encode())

                else:
                    print(f"Unexpected server message: {data}")
                    conn.send("GET_JOB".encode())

        except ConnectionResetError:
            print("Server forcibly disconnected. Reconnecting in 3 seconds...")
        except Exception as e:
            print(f"Connection lost: {e}. Reconnecting in 3 seconds...")
        finally:
            try:
                conn.close()
            except Exception:
                pass
        time.sleep(3)


run()