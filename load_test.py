import socket
import ssl
import threading
import time
import csv
import random

HOST = "127.0.0.1"
PORT = 5000

context = ssl._create_unverified_context()

OPERATIONS = ["ADD", "SUB", "MUL", "DIV"]
LOAD_LEVELS = [10, 20, 50, 100]
CSV_FILE = "performance_results.csv"


def submit_job(job, results_list, index):
    secure_client = None
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        secure_client = context.wrap_socket(client)
        secure_client.settimeout(360)
        secure_client.connect((HOST, PORT))

        submitted_at = time.time()
        secure_client.send(f"SUBMIT {job}".encode())

        try:
            accepted = secure_client.recv(1024).decode().strip()
        except socket.timeout:
            print(f"Timed out waiting for JOB_ACCEPTED for job: {job}")
            results_list[index] = None
            return

        if not accepted.startswith("JOB_ACCEPTED"):
            print(f"Job rejected by server: {accepted} for job: {job}")
            results_list[index] = None
            return

        try:
            result_msg = secure_client.recv(1024).decode().strip()
        except socket.timeout:
            print(f"Timed out waiting for result for job: {job}")
            results_list[index] = None
            return

        result_received_at = time.time()

        if "TIMEOUT" in result_msg:
            print(f"Server reported timeout for job: {job}")
            results_list[index] = None
            return

        response_time = round(result_received_at - submitted_at, 4)

        true_queue_wait = None
        true_processing = None
        parts = result_msg.split()
        for part in parts:
            if part.startswith("queue_wait="):
                try:
                    true_queue_wait = float(part.split("=")[1])
                except ValueError:
                    pass
            if part.startswith("processing="):
                try:
                    true_processing = float(part.split("=")[1])
                except ValueError:
                    pass

        results_list[index] = {
            "job": job,
            "response_time": response_time,
            "queue_wait_time": true_queue_wait if true_queue_wait is not None else 0,
            "processing_latency": true_processing if true_processing is not None else 0,
        }

    except ssl.SSLError as e:
        print(f"SSL error for job {job}: {e}")
        results_list[index] = None
    except ConnectionRefusedError:
        print(f"Could not connect to server for job: {job}. Is server.py running?")
        results_list[index] = None
    except ConnectionResetError:
        print(f"Connection reset by server for job: {job}")
        results_list[index] = None
    except Exception as e:
        print(f"Unexpected error for job {job}: {e}")
        results_list[index] = None
    finally:
        if secure_client:
            try:
                secure_client.close()
            except Exception:
                pass


def generate_jobs(num_jobs):
    jobs = []
    for _ in range(num_jobs):
        op = random.choice(OPERATIONS)
        a = random.randint(1, 100)
        b = random.randint(1, 100)
        jobs.append(f"{op} {a} {b}")
    return jobs


def run_load_test(num_jobs):
    print(f"\n{'='*50}")
    print(f"Running load test with {num_jobs} concurrent jobs...")
    print(f"{'='*50}")

    jobs = generate_jobs(num_jobs)
    results_list = [None] * num_jobs
    threads = []

    batch_start = time.time()

    for i, job in enumerate(jobs):
        t = threading.Thread(target=submit_job, args=(job, results_list, i), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=400)

    batch_end = time.time()
    total_time = batch_end - batch_start

    completed = [r for r in results_list if r is not None]
    failed = num_jobs - len(completed)

    if not completed:
        print(f"No jobs completed for load level {num_jobs}")
        return None

    avg_response_time = sum(r["response_time"] for r in completed) / len(completed)
    avg_queue_wait = sum(r["queue_wait_time"] for r in completed) / len(completed)
    avg_processing = sum(r["processing_latency"] for r in completed) / len(completed)
    throughput = len(completed) / total_time

    print(f"Jobs Submitted:          {num_jobs}")
    print(f"Jobs Completed:          {len(completed)}")
    print(f"Jobs Failed:             {failed}")
    print(f"Total Time:              {round(total_time, 4)}s")
    print(f"Throughput:              {round(throughput, 4)} jobs/sec")
    print(f"Avg Response Time:       {round(avg_response_time, 4)}s")
    print(f"Avg Queue Wait Time:     {round(avg_queue_wait, 4)}s  (submit → worker picks up)")
    print(f"Avg Processing Latency:  {round(avg_processing, 4)}s  (worker picks up → finishes)")

    return {
        "num_jobs": num_jobs,
        "completed": len(completed),
        "failed": failed,
        "total_time_s": round(total_time, 4),
        "throughput_jobs_per_sec": round(throughput, 4),
        "avg_response_time_s": round(avg_response_time, 4),
        "avg_queue_wait_time_s": round(avg_queue_wait, 4),
        "avg_processing_latency_s": round(avg_processing, 4)
    }


def save_to_csv(all_results):
    fieldnames = [
        "num_jobs", "completed", "failed", "total_time_s",
        "throughput_jobs_per_sec", "avg_response_time_s",
        "avg_queue_wait_time_s", "avg_processing_latency_s"
    ]
    try:
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\nResults saved to {CSV_FILE}")
    except IOError as e:
        print(f"Failed to save CSV: {e}")


def main():
    print("Distributed Job Queue - Performance Evaluation")
    print(f"Load levels: {LOAD_LEVELS} jobs")
    print("Make sure server.py and at least 2 worker.py instances are running.\n")

    try:
        input("Press Enter to start the performance tests...")
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return

    all_results = []

    for load in LOAD_LEVELS:
        try:
            result = run_load_test(load)
            if result:
                all_results.append(result)
        except KeyboardInterrupt:
            print("\nLoad test interrupted.")
            break
        time.sleep(3)

    if all_results:
        save_to_csv(all_results)

        print(f"\n{'='*50}")
        print("SUMMARY TABLE")
        print(f"{'='*50}")
        print(f"{'Jobs':<8} {'Throughput':<15} {'Avg Response':<15} {'Avg Queue Wait':<18} {'Avg Processing'}")
        print(f"{'-'*75}")
        for r in all_results:
            print(f"{r['num_jobs']:<8} {r['throughput_jobs_per_sec']:<15} {r['avg_response_time_s']:<15} {r['avg_queue_wait_time_s']:<18} {r['avg_processing_latency_s']}")
    else:
        print("No results to display.")


main()
