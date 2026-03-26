# Distributed Secure Job Queue System (TCP)

## Overview
This project implements a distributed job queue system using socket programming in Python. The system follows a client-server architecture where multiple clients can submit jobs, and multiple worker nodes process those jobs concurrently.

The main goal of this project is to ensure reliable job execution, proper synchronization between components, and support for multiple clients and workers at the same time.

---

## Problem Statement
Design and implement a socket-based distributed job queue system where multiple clients submit jobs and multiple worker nodes fetch and execute them. The system should ensure that each job is processed exactly once and should support concurrent job submission and execution.

---

## System Architecture

The system consists of three main components:

### 1. Client
- Sends job requests to the server  
- Can also query the status of a submitted job  
- Example: `SUBMIT ADD 5 6`, `QUERY 1`

### 2. Server
- Acts as the central coordinator  
- Maintains a job queue  
- Assigns jobs to available workers  
- Tracks job status (pending, in-progress, completed)  
- Handles multiple clients and workers simultaneously using threads  

### 3. Worker
- Requests jobs from the server  
- Processes the assigned job  
- Sends the result back to the server  

This design ensures centralized control and avoids duplicate job execution.

---

## Features
- Supports multiple clients submitting jobs at the same time  
- Multiple workers process jobs in parallel  
- Centralized job queue for proper synchronization  
- Fault tolerance (jobs are re-queued if a worker fails)  
- Job status tracking using QUERY command  
- Secure communication using SSL/TLS  
- Performance testing using load simulation  

---

## Technologies Used
- Python  
- TCP Socket Programming  
- SSL/TLS for secure communication  
- Multithreading  

---

## Setup Instructions

### Requirements
- Python 3 installed  
- VS Code or any IDE  

### Step 1: Download the Project
Place all files (`server.py`, `client.py`, `worker.py`, etc.) in the same folder.

### Step 2: Generate SSL Certificates
Run the following command in terminal:


openssl req -new -x509 -days 365 -nodes -out cert.pem -keyout key.pem


This creates:
- `cert.pem` (certificate)
- `key.pem` (private key)

These are required for secure communication between components.

---

## How to Run the System

Open multiple terminals (Command Prompt or VS Code terminal).

### Step 1: Start the Server

python server.py


### Step 2: Start Worker Nodes
Run this in one or more terminals:

python worker.py


### Step 3: Start Client

python client.py


---

## Usage Instructions

### Submitting Jobs

SUBMIT ADD 5 6
SUBMIT MUL 3 4


### Query Job Status

QUERY 1


### Exit

EXIT


---

## Performance Evaluation

To test the system under different loads:


python load_test.py


This script measures:
- Response time  
- Throughput  
- Latency  
- Queue waiting time  

The results are stored in:

performance_results.csv


---

## Fault Tolerance
The system handles failures in the following ways:
- If a worker crashes, the job is re-added to the queue  
- Stale jobs are automatically reassigned  
- Duplicate processing is avoided  

---

## Expected Outcome
The system successfully handles multiple clients and workers simultaneously, ensures reliable job execution, and maintains stable performance under different workloads.

---

## Notes
- SSL uses self-signed certificates for demonstration purposes  
- The system is designed to run on localhost  
- Increasing the number of workers improves performance
