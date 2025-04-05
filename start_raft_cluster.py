#!/usr/bin/env python3
import subprocess
import time
import sys
import os
import argparse
import signal
import socket # For hostname

# Default configuration
DEFAULT_BASE_PORT = 60061
DEFAULT_NODE_COUNT = 3

# Global list to keep track of processes for cleanup
processes = []

def check_prerequisites():
    """Check if necessary files exist."""
    # Check if raft_video_processor.py exists
    if not os.path.exists("raft_video_processor.py"):
        print("Error: raft_video_processor.py not found.")
        print("Make sure you're running this script from the project root directory.")
        return False

    # Check if Proto files are generated
    proto_files = ["raft_pb2.py", "raft_pb2_grpc.py", "video_stream_pb2.py", "video_stream_pb2_grpc.py"]
    missing_files = [f for f in proto_files if not os.path.exists(f)]
    if missing_files:
        print("Error: Some generated proto files are missing.")
        print("Missing files:", ", ".join(missing_files))
        print("Please generate them first using the Makefile or:")
        print("  python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. *.proto")
        return False
    return True

def start_raft_node(node_id, port, peers_str):
    """Starts a single RAFT node process."""
    cmd = [
        sys.executable, # Use the same python interpreter
        "raft_video_processor.py",
        "--id", str(node_id),
        "--port", str(port),
        "--peers", peers_str
    ]
    
    # Start the process in the background. Redirect output if desired.
    # For simplicity here, let them print to the main console.
    # If output needs capturing/prefixing, more complex handling is needed.
    print(f"  Starting Node {node_id} on port {port} with peers: {peers_str}")
    proc = subprocess.Popen(cmd)
    return proc

def signal_handler(sig, frame):
    """Handles Ctrl+C for graceful shutdown."""
    print("\nCtrl+C received. Shutting down RAFT cluster...")
    for proc in processes:
        if proc.poll() is None: # If process is still running
            print(f"  Terminating process {proc.pid}...")
            proc.terminate()
    
    # Wait for all processes to terminate
    print("Waiting for processes to finish...")
    for proc in processes:
        try:
            proc.wait(timeout=5) # Wait up to 5 seconds per process
        except subprocess.TimeoutExpired:
            print(f"  Process {proc.pid} did not terminate gracefully, killing.")
            proc.kill()
            proc.wait() # Wait for kill
        except Exception as e:
             print(f"Error waiting for process {proc.pid}: {e}") # Handle potential errors
    
    print("All nodes stopped.")
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description='Start a RAFT cluster for the Parking System')
    parser.add_argument('--nodes', type=int, default=DEFAULT_NODE_COUNT,
                        help=f'Number of RAFT nodes to start (default: {DEFAULT_NODE_COUNT})')
    parser.add_argument('--base-port', type=int, default=DEFAULT_BASE_PORT,
                        help=f'Base port number for node 1 (default: {DEFAULT_BASE_PORT})')
    args = parser.parse_args()

    if not check_prerequisites():
        sys.exit(1)

    node_count = args.nodes
    base_port = args.base_port
    hostname = socket.gethostname() # Get local hostname

    print(f"Starting a RAFT cluster with {node_count} nodes on host '{hostname}'")
    print(f"Ports range from {base_port} to {base_port + node_count - 1}")

    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    # Start all nodes
    try:
        for i in range(1, node_count + 1):
            current_port = base_port + i - 1
            # Generate peer string for the current node
            peers = []
            for j in range(1, node_count + 1):
                if i != j:
                    peer_port = base_port + j - 1
                    peers.append(f"{j}:{hostname}:{peer_port}")
            peers_str = ",".join(peers)
            
            proc = start_raft_node(i, current_port, peers_str)
            processes.append(proc)
            time.sleep(0.5) # Small delay between starting nodes
        
        print(f"\n{node_count} RAFT nodes started. Cluster is running.")
        print("Press Ctrl+C to stop all nodes.")
        
        # Keep the main script alive while nodes run in the background
        # Check periodically if any process exited unexpectedly
        while True:
            for i, proc in enumerate(processes):
                 if proc.poll() is not None: # Check if the process terminated
                    print(f"\nERROR: Node {i+1} (PID {proc.pid}) terminated unexpectedly with code {proc.returncode}.")
                    print("Shutting down remaining nodes...")
                    # Trigger cleanup
                    signal_handler(signal.SIGINT, None) 
                    # Should exit via signal_handler's sys.exit
            time.sleep(5) # Check every 5 seconds

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        # Attempt cleanup even on unexpected errors
        signal_handler(signal.SIGINT, None) 

if __name__ == '__main__':
    main() 