#!/usr/bin/env python3
import subprocess
import threading
import time
import sys
import os
import argparse
import signal

# Default configuration
DEFAULT_BASE_PORT = 60061
DEFAULT_NODE_COUNT = 3

def start_node(node_id, base_port, node_count):
    """Start a RAFT video processor node in a separate process."""
    peers = []
    for i in range(1, node_count + 1):
        if i != node_id:
            peers.append(f"{i}:localhost:{base_port + i - 1}")
    
    peer_string = ",".join(peers)
    port = base_port + node_id - 1
    
    cmd = [
        sys.executable,
        "raft_video_processor.py",
        "--id", str(node_id),
        "--port", str(port),
        "--peers", peer_string
    ]
    
    proc = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    # Print output with node ID prefix
    prefix = f"[Node {node_id}] "
    for line in proc.stdout:
        print(f"{prefix}{line}", end="")
    
    return proc

def main():
    parser = argparse.ArgumentParser(description='Start a RAFT cluster with multiple nodes')
    parser.add_argument('--nodes', type=int, default=DEFAULT_NODE_COUNT, 
                        help=f'Number of nodes to start (default: {DEFAULT_NODE_COUNT})')
    parser.add_argument('--base-port', type=int, default=DEFAULT_BASE_PORT,
                        help=f'Base port number (default: {DEFAULT_BASE_PORT})')
    args = parser.parse_args()
    
    # Check if raft_video_processor.py exists
    if not os.path.exists("raft_video_processor.py"):
        print("Error: raft_video_processor.py not found.")
        print("Make sure you're running this script from the project root directory.")
        sys.exit(1)
    
    # Check if Proto files are generated
    proto_files = ["raft_pb2.py", "raft_pb2_grpc.py", "video_stream_pb2.py", "video_stream_pb2_grpc.py"]
    missing_files = [f for f in proto_files if not os.path.exists(f)]
    if missing_files:
        print("Error: Some generated proto files are missing.")
        print("Missing files:", ", ".join(missing_files))
        print("Please generate them first with:")
        print("  python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto")
        print("  python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. video_stream.proto")
        sys.exit(1)
    
    node_count = args.nodes
    base_port = args.base_port
    
    print(f"Starting a RAFT cluster with {node_count} nodes")
    print(f"Ports: {', '.join(str(base_port + i) for i in range(node_count))}")
    
    # Start all nodes
    processes = []
    try:
        for i in range(1, node_count + 1):
            proc = start_node(i, base_port, node_count)
            processes.append(proc)
            # Small delay to avoid output confusion
            time.sleep(0.5)
        
        # Wait for Ctrl+C
        print("\nRAFT cluster is running. Press Ctrl+C to stop all nodes.\n")
        signal.pause()
        
    except KeyboardInterrupt:
        print("\nShutting down RAFT cluster...")
    finally:
        # Terminate all processes
        for proc in processes:
            if proc.poll() is None:  # If process is still running
                proc.terminate()
        
        # Wait for all processes to terminate
        for proc in processes:
            proc.wait()
        
        print("All nodes stopped.")

if __name__ == "__main__":
    main() 