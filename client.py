import socket
import ssl

HOST = "127.0.0.1"
PORT = 5000

context = ssl._create_unverified_context()

VALID_OPERATIONS = {"ADD", "SUB", "MUL", "DIV"}


def validate_input(parts):
    if len(parts) != 3:
        return False, "Invalid format. Use: SUBMIT <OP> <num> <num>"
    op, a, b = parts
    if op not in VALID_OPERATIONS:
        return False, f"Unknown operation '{op}'. Valid: ADD SUB MUL DIV"
    try:
        int(a)
        int(b)
    except ValueError:
        return False, "Operands must be integers"
    return True, None


def submit_job(job):
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        secure_client = context.wrap_socket(client)
        secure_client.settimeout(360)
        secure_client.connect((HOST, PORT))
    except ssl.SSLError as e:
        print(f"SSL error: {e}")
        return
    except ConnectionRefusedError:
        print("Could not connect to server. Is server.py running?")
        return
    except Exception as e:
        print(f"Connection error: {e}")
        return

    try:
        secure_client.send(f"SUBMIT {job}".encode())

        response = secure_client.recv(1024).decode()
        print(f"Server: {response}")

        if response.startswith("ERROR"):
            return

        if response.startswith("JOB_ACCEPTED"):
            try:
                result = secure_client.recv(1024).decode()
                print(f"Server: {result}")
            except socket.timeout:
                print("Timed out waiting for result.")
    except Exception as e:
        print(f"Error during job submission: {e}")
    finally:
        try:
            secure_client.close()
        except Exception:
            pass


def query_job(jid):
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        secure_client = context.wrap_socket(client)
        secure_client.settimeout(10)
        secure_client.connect((HOST, PORT))
        secure_client.send(f"QUERY {jid}".encode())
        response = secure_client.recv(1024).decode()
        print(f"Server: {response}")
    except ConnectionRefusedError:
        print("Could not connect to server. Is server.py running?")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        try:
            secure_client.close()
        except Exception:
            pass


def main():
    print("Commands: SUBMIT <OP> <a> <b>  |  QUERY <job_id>  |  EXIT")
    print("Example:  SUBMIT ADD 5 6")

    while True:
        try:
            user_input = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not user_input:
            continue

        parts = user_input.split()
        command = parts[0].upper()

        if command == "EXIT":
            break

        elif command == "SUBMIT":
            job_parts = parts[1:]
            valid, error = validate_input(job_parts)
            if not valid:
                print(f"Error: {error}")
                continue
            job = " ".join(job_parts)
            submit_job(job)

        elif command == "QUERY":
            if len(parts) != 2:
                print("Usage: QUERY <job_id>")
                continue
            try:
                int(parts[1])
            except ValueError:
                print("Job ID must be an integer")
                continue
            query_job(parts[1])

        else:
            print("Unknown command. Use SUBMIT, QUERY, or EXIT")


main()