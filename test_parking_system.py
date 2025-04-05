import subprocess
import time
import sys
import signal
import os
import re
import socket
from typing import List, Dict, Tuple

# --- Configuration ---
LOG_DIR = "test_logs"
RAFT_NODES_TO_START = 3
CAMERAS_TO_START = 3
RAFT_BASE_PORT = 60060
STABILITY_TEST_DURATION = 15 # seconds
FAILURE_OBSERVE_DURATION = 15 # seconds
RECOVERY_OBSERVE_DURATION = 15 # seconds
# --- End Configuration ---

# Ensure log directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

class SystemStatus:
    def __init__(self):
        self.raft_nodes: Dict[int, Dict] = {}  # node_id -> {"process": Popen, "stdout": file, "stderr": file, "port": int}
        self.cameras: Dict[int, Dict] = {}     # camera_id -> {"process": Popen, "stdout": file, "stderr": file}
        self.running = True
        self.leader_node_id = -1 # Track the current leader
        signal.signal(signal.SIGINT, self.cleanup)

    def _get_raft_peers_str(self, node_id: int, num_nodes: int, base_port: int) -> str:
        return ",".join(f"{j}:localhost:{base_port + j}" 
                        for j in range(1, num_nodes + 1) if j != node_id)

    def start_raft_node(self, node_id: int, num_nodes: int = RAFT_NODES_TO_START, base_port: int = RAFT_BASE_PORT):
        if node_id in self.raft_nodes:
            print(f"RAFT node {node_id} already running.")
            return
        port = base_port + node_id
        peers = self._get_raft_peers_str(node_id, num_nodes, base_port)
        cmd = [sys.executable, "raft_video_processor.py", "--id", str(node_id), "--port", str(port), "--peers", peers]
        
        # Use unique log filenames
        log_file_basename = f"raft_node_{node_id}"
        stdout_log_path = os.path.join(LOG_DIR, f"{log_file_basename}_stdout.log")
        stderr_log_path = os.path.join(LOG_DIR, f"{log_file_basename}_stderr.log")
        
        # Ensure the log files are empty/new
        open(stdout_log_path, "w").close()
        open(stderr_log_path, "w").close()
        
        # Now open for the process
        stdout_log = open(stdout_log_path, "w")
        stderr_log = open(stderr_log_path, "w")
        
        process = subprocess.Popen(cmd, stdout=stdout_log, stderr=stderr_log, text=True)
        self.raft_nodes[node_id] = {"process": process, "stdout": stdout_log, "stderr": stderr_log, "port": port}
        print(f"  Started RAFT node {node_id} on port {port} (Logs: {LOG_DIR}/{log_file_basename}_*.log)")
        stdout_log.flush()  # Ensure log is flushed to disk
        time.sleep(1) # Basic startup time

    def start_camera(self, camera_id: int):
        if camera_id in self.cameras:
            print(f"Camera {camera_id} already running.")
            return
        cmd = [sys.executable, "camera_video_client.py", "--camera-id", str(camera_id), "--frame-interval", "10"]
        
        # Use unique log filenames
        log_file_basename = f"camera_{camera_id}"
        stdout_log_path = os.path.join(LOG_DIR, f"{log_file_basename}_stdout.log")
        stderr_log_path = os.path.join(LOG_DIR, f"{log_file_basename}_stderr.log")
        
        # Ensure the log files are empty/new
        open(stdout_log_path, "w").close()
        open(stderr_log_path, "w").close()
        
        # Now open for the process
        stdout_log = open(stdout_log_path, "w")
        stderr_log = open(stderr_log_path, "w")
        
        process = subprocess.Popen(cmd, stdout=stdout_log, stderr=stderr_log, text=True)
        self.cameras[camera_id] = {"process": process, "stdout": stdout_log, "stderr": stderr_log}
        print(f"  Started camera {camera_id} (Logs: {LOG_DIR}/{log_file_basename}_*.log)")
        stdout_log.flush()  # Ensure log is flushed to disk
        time.sleep(1) # Basic startup time

    def stop_node(self, process_dict: Dict, process_id: int, process_type: str):
         if process_id in process_dict:
            print(f"  Stopping {process_type} {process_id}")
            info = process_dict[process_id]
            if info["process"].poll() is None: # Only terminate if running
                info["process"].terminate()
                try:
                    info["process"].wait(timeout=5) # Wait max 5s
                except subprocess.TimeoutExpired:
                    print(f"  WARNING: {process_type} {process_id} did not terminate gracefully, killing.")
                    info["process"].kill()
                    info["process"].wait() # Wait for kill
            # Close logs even if process already terminated
            if not info["stdout"].closed: info["stdout"].close()
            if not info["stderr"].closed: info["stderr"].close()
            del process_dict[process_id]
            # If a leader was stopped, reset leader tracking
            if process_type == "RAFT node" and self.leader_node_id == process_id:
                self.leader_node_id = -1
         else:
             print(f"  {process_type} {process_id} not found or already stopped.")


    def stop_raft_node(self, node_id: int):
        self.stop_node(self.raft_nodes, node_id, "RAFT node")

    def stop_camera(self, camera_id: int):
        self.stop_node(self.cameras, camera_id, "Camera")

    def check_for_leader(self) -> bool:
        """Check logs for leader election messages."""
        leader_found = False
        # Reset leader ID before check
        self.leader_node_id = -1
        
        # First, make sure all logs are flushed to disk
        for node_id, info in self.raft_nodes.items():
            if not info["stdout"].closed:
                info["stdout"].flush()
        
        active_raft_nodes = {nid: info for nid, info in self.raft_nodes.items() if info["process"].poll() is None}
        
        for node_id, info in active_raft_nodes.items():
             try:
                 # Check the log file directly from disk
                 log_file_path = info["stdout"].name
                 print(f"  Debug: Checking log {log_file_path}")
                 
                 # Check if file exists and has content
                 if os.path.exists(log_file_path):
                     file_size = os.path.getsize(log_file_path)
                     print(f"  Debug: File size is {file_size} bytes")
                     
                     if file_size > 0:
                         with open(log_file_path, "r") as f:
                             lines = f.readlines()
                             print(f"  Debug: Read {len(lines)} lines from {log_file_path}")
                             
                             if lines:
                                 print(f"  Debug: First line: {lines[0].strip()}")
                                 print(f"  Debug: Last line: {lines[-1].strip()}")
                                 
                                 for line in lines:
                                     clean_line = line.strip()  # Strip whitespace
                                     if "became leader for term" in clean_line.lower():  # Case-insensitive search
                                         # Extract leader ID from the node ID running the process
                                         self.leader_node_id = node_id  # Assume node logging this is leader
                                         print(f"  Detected RAFT Leader: Node {node_id} from log: {log_file_path}")
                                         print(f"  Leader line: {clean_line}")
                                         leader_found = True
                                         break  # Found leader in this file
                     else:
                         print(f"  Debug: File {log_file_path} is empty")
                 else:
                     print(f"  Debug: File {log_file_path} does not exist")
                     
                 if leader_found:
                     break # Found leader in the cluster
             except FileNotFoundError:
                 print(f"  Warning: Log file not found for node {node_id}: {log_file_path}")
                 continue # Log file might not exist if node failed quickly
             except Exception as e:
                 print(f"  Warning: Error reading log for node {node_id} ({log_file_path}): {e}")
                 import traceback
                 print(f"  Debug: {traceback.format_exc()}")

        if not leader_found and active_raft_nodes:
             print("  Warning: Could not detect RAFT leader from logs.")

        # Return true if a leader was found OR if there are no active raft nodes left
        return leader_found or not active_raft_nodes

    def monitor_activity(self, duration: int):
        """Monitor logs for basic activity during the stability test."""
        print(f"  Monitoring system activity for {duration} seconds...")
        end_time = time.time() + duration
        last_status_check = 0
        logs_to_tail = {}

        # Prepare log files for tailing
        for cam_id, cam_info in self.cameras.items():
            logs_to_tail[f"CAM_{cam_id}"] = open(cam_info["stdout"].name, "r")
        for node_id, node_info in self.raft_nodes.items():
             # Primarily monitor leader's stdout if known, otherwise monitor all
             if self.leader_node_id == -1 or node_id == self.leader_node_id:
                logs_to_tail[f"RAFT_{node_id}"] = open(node_info["stdout"].name, "r")

        # Seek to the end of files initially
        for f in logs_to_tail.values():
            f.seek(0, os.SEEK_END)

        output_buffer = ""
        while time.time() < end_time and self.running:
            activity_detected = False
            current_output = ""
            for name, f in logs_to_tail.items():
                try:
                    line = f.readline()
                    if line:
                        activity_detected = True
                        if "CAM" in name and "Sending frame" in line:
                           current_output += "C" # Camera Sent
                        elif "CAM" in name and "Received ack" in line:
                            current_output += "A" # Camera Ack
                        elif "RAFT" in name and "[LEADER" in line and "Processed Frame" in line:
                            match = re.search(r"Camera (\d+).*Spaces: (\d+)", line)
                            if match:
                                current_output += f"L(C{match.group(1)}->S{match.group(2)})" # Leader Processed
                            else:
                                current_output += "L" # Leader generic activity
                        # Add more patterns for heartbeats if needed
                except Exception as e:
                    print(f"\nError reading log {name}: {e}")
                    # Attempt to reopen? For now, just ignore.

            if current_output:
                 output_buffer += current_output + " "

            # Print buffered output periodically or when buffer is full
            if len(output_buffer) > 80 or (activity_detected and time.time() - last_status_check > 2):
                print(f"    Activity: {output_buffer.strip()}")
                output_buffer = ""
                last_status_check = time.time()

            # Check for unexpected terminations during monitoring
            if self.check_unexpected_terminations():
                 print("  ERROR: Unexpected process termination during monitoring!")
                 break # Stop monitoring early

            if not activity_detected and output_buffer == "":
                time.sleep(0.5) # Sleep if no activity

        print("  Monitoring complete.")
        # Close tailed files
        for f in logs_to_tail.values():
            f.close()


    def check_unexpected_terminations(self) -> bool:
        """Check if any managed process terminated unexpectedly."""
        terminated = False
        for node_id, info in list(self.raft_nodes.items()):
            if info["process"].poll() is not None:
                print(f"  ** WARNING: RAFT node {node_id} terminated unexpectedly (Exit Code: {info['process'].returncode}) **")
                # Don't stop it here, let the test logic decide pass/fail
                terminated = True
        for camera_id, info in list(self.cameras.items()):
             if info["process"].poll() is not None:
                print(f"  ** WARNING: Camera {camera_id} terminated unexpectedly (Exit Code: {info['process'].returncode}) **")
                # Don't stop it here
                terminated = True
        return terminated

    def get_status(self) -> Tuple[int, int]:
        """Return the count of currently running RAFT nodes and cameras."""
        # Ensure counts reflect reality by checking poll()
        running_rafts = sum(1 for info in self.raft_nodes.values() if info["process"].poll() is None)
        running_cameras = sum(1 for info in self.cameras.values() if info["process"].poll() is None)
        return running_rafts, running_cameras

    def cleanup(self, signum=None, frame=None):
        if not self.running: return # Prevent double cleanup
        print("\nCleaning up all processes...")
        self.running = False
        # Use lists to avoid modifying dict during iteration
        for camera_id in list(self.cameras.keys()):
            self.stop_camera(camera_id)
        for node_id in list(self.raft_nodes.keys()):
            self.stop_raft_node(node_id)
        print(f"Cleanup complete. Log files are in '{LOG_DIR}'.")
        # Optional: sys.exit(0) if running as standalone script and want to force exit

# --- Test Case Functions ---

def run_test_case(label: str, description: str, system: SystemStatus, actions: callable, check: callable) -> bool:
    """Runs a single test case."""
    print(f"\n--- {label}: {description} ---")
    initial_rafts, initial_cameras = system.get_status()
    print(f"  Initial State: {initial_rafts} RAFT nodes, {initial_cameras} cameras running.")

    try:
        actions(system) # Perform test actions
        passed = check(system) # Perform checks
    except Exception as e:
        print(f"  ERROR during test execution: {e}")
        passed = False

    # Final status check for unexpected issues
    if system.check_unexpected_terminations() and passed:
         print("  WARNING: Test passed check, but unexpected terminations occurred.")
         # Depending on strictness, could mark as fail here: passed = False

    final_rafts, final_cameras = system.get_status()
    print(f"  Final State: {final_rafts} RAFT nodes, {final_cameras} cameras running.")
    result = "PASS" if passed else "FAIL"
    print(f"--- Result: {result} ---")
    return passed

def actions_stability(system: SystemStatus):
    print("  Action: Monitoring system activity.")
    system.monitor_activity(STABILITY_TEST_DURATION)
    # Flush all logs before checking
    for node_id, node_info in system.raft_nodes.items():
        node_info["stdout"].flush()

def check_stability(system: SystemStatus) -> bool:
    rafts, cameras = system.get_status()
    leader_ok = system.check_for_leader() # Check if a leader exists or was detected
    # Basic check: ensure all expected processes are still running and leader detected
    passed = (rafts == RAFT_NODES_TO_START and
              cameras == CAMERAS_TO_START and
              leader_ok and
              not system.check_unexpected_terminations()) # Re-check terminations
    if not passed:
        print(f"  Check Failed: Expected {RAFT_NODES_TO_START} RAFT, {CAMERAS_TO_START} CAM, Leader OK. Got {rafts} RAFT, {cameras} CAM, Leader OK: {leader_ok}")
    return passed

def actions_raft_failure(system: SystemStatus):
    target_node = 1 # Target node 1 for failure
    print(f"  Action: Stopping RAFT node {target_node}.")
    system.stop_raft_node(target_node)
    print(f"  Action: Observing system for {FAILURE_OBSERVE_DURATION} seconds post-failure.")
    time.sleep(FAILURE_OBSERVE_DURATION)
    # Flush all logs before checking
    for node_id, node_info in system.raft_nodes.items():
        node_info["stdout"].flush()

def check_raft_failure(system: SystemStatus) -> bool:
    rafts, cameras = system.get_status()
    leader_ok = system.check_for_leader() # Check if remaining nodes elected a new leader
    passed = (rafts == RAFT_NODES_TO_START - 1 and
              cameras == CAMERAS_TO_START and # Cameras should remain
              leader_ok and
              not system.check_unexpected_terminations())
    if not passed:
         print(f"  Check Failed: Expected {RAFT_NODES_TO_START - 1} RAFT, {CAMERAS_TO_START} CAM, Leader OK. Got {rafts} RAFT, {cameras} CAM, Leader OK: {leader_ok}")
    return passed

def actions_camera_failure(system: SystemStatus):
    target_camera = 1 # Target camera 1
    print(f"  Action: Stopping Camera {target_camera}.")
    system.stop_camera(target_camera)
    print(f"  Action: Observing system for {FAILURE_OBSERVE_DURATION} seconds post-failure.")
    time.sleep(FAILURE_OBSERVE_DURATION)
    # Flush all logs before checking
    for node_id, node_info in system.raft_nodes.items():
        node_info["stdout"].flush()

def check_camera_failure(system: SystemStatus) -> bool:
    rafts, cameras = system.get_status()
    leader_ok = system.check_for_leader() # Leader should be unaffected
    # RAFT nodes should be same as *after RAFT failure test*
    expected_rafts = RAFT_NODES_TO_START -1
    passed = (rafts == expected_rafts and
              cameras == CAMERAS_TO_START - 1 and # One less camera
              leader_ok and
              not system.check_unexpected_terminations())
    if not passed:
         print(f"  Check Failed: Expected {expected_rafts} RAFT, {CAMERAS_TO_START - 1} CAM, Leader OK. Got {rafts} RAFT, {cameras} CAM, Leader OK: {leader_ok}")
    return passed

def actions_raft_recovery(system: SystemStatus):
    target_node = 1 # Node that was stopped
    print(f"  Action: Restarting RAFT node {target_node}.")
    system.start_raft_node(target_node) # Restart the failed node
    print(f"  Action: Observing system for {RECOVERY_OBSERVE_DURATION} seconds post-recovery.")
    time.sleep(RECOVERY_OBSERVE_DURATION)
    # Flush all logs before checking
    for node_id, node_info in system.raft_nodes.items():
        node_info["stdout"].flush()

def check_raft_recovery(system: SystemStatus) -> bool:
    rafts, cameras = system.get_status()
    leader_ok = system.check_for_leader() # Check leader status after recovery
    # Expect counts to be back to original RAFT, but still one camera down
    expected_cameras = CAMERAS_TO_START - 1
    passed = (rafts == RAFT_NODES_TO_START and
              cameras == expected_cameras and
              leader_ok and
              not system.check_unexpected_terminations())
    if not passed:
         print(f"  Check Failed: Expected {RAFT_NODES_TO_START} RAFT, {expected_cameras} CAM, Leader OK. Got {rafts} RAFT, {cameras} CAM, Leader OK: {leader_ok}")

    return passed

def actions_camera_recovery(system: SystemStatus):
    target_camera = 1 # Camera that was stopped
    print(f"  Action: Restarting Camera {target_camera}.")
    system.start_camera(target_camera) # Restart the failed camera
    print(f"  Action: Observing system for {RECOVERY_OBSERVE_DURATION} seconds post-recovery.")
    time.sleep(RECOVERY_OBSERVE_DURATION)
    # Flush all logs before checking
    for node_id, node_info in system.raft_nodes.items():
        node_info["stdout"].flush()

def check_camera_recovery(system: SystemStatus) -> bool:
    rafts, cameras = system.get_status()
    leader_ok = system.check_for_leader() # Leader should still be fine
    # Everything should be back to original counts
    passed = (rafts == RAFT_NODES_TO_START and
              cameras == CAMERAS_TO_START and
              leader_ok and
              not system.check_unexpected_terminations())
    if not passed:
         print(f"  Check Failed: Expected {RAFT_NODES_TO_START} RAFT, {CAMERAS_TO_START} CAM, Leader OK. Got {rafts} RAFT, {cameras} CAM, Leader OK: {leader_ok}")
    return passed


# --- Main Execution ---

TEST_PLAN = [
    {"label": "Test #1", "description": "System Stability & Activity Monitoring", "actions": actions_stability, "check": check_stability},
    {"label": "Test #2", "description": "RAFT Node Failure Tolerance", "actions": actions_raft_failure, "check": check_raft_failure},
    {"label": "Test #3", "description": "Camera Failure Tolerance", "actions": actions_camera_failure, "check": check_camera_failure},
    {"label": "Test #4", "description": "RAFT Node Recovery", "actions": actions_raft_recovery, "check": check_raft_recovery},
    {"label": "Test #5", "description": "Camera Recovery", "actions": actions_camera_recovery, "check": check_camera_recovery},
]

def main():
    print("="*50)
    print(" Parking System Test Suite")
    print("="*50)
    print("\nTest Plan:")
    for test in TEST_PLAN:
        print(f"  - {test['label']}: {test['description']}")
    print("\nStarting system setup...")

    system = SystemStatus()
    all_passed = True

    try:
        # Initial Setup
        print("\nSetting up initial system state...")
        for i in range(1, RAFT_NODES_TO_START + 1):
            system.start_raft_node(i)
        print("Waiting for RAFT cluster to potentially elect a leader...")
        time.sleep(10) # Allow more time for initial election
        # Try multiple times to find leader
        leader_found = False
        max_attempts = 5
        for attempt in range(max_attempts):
            # Flush all RAFT node logs before checking for leader
            for node_id, node_info in system.raft_nodes.items():
                node_info["stdout"].flush()
            if system.check_for_leader():  # Returns True if leader found
                leader_found = True
                print(f"  Leader detected after {attempt+1} attempts")
                break
            if attempt < max_attempts - 1:  # Don't sleep after last attempt
                print(f"  No leader detected, retrying in 2 seconds (attempt {attempt+1}/{max_attempts})...")
                time.sleep(2)
        if not leader_found:
            print("  Warning: No RAFT leader detected after multiple attempts. Tests may fail.")

        for i in range(1, CAMERAS_TO_START + 1):
            system.start_camera(i)
        print("Waiting for cameras to connect...")
        time.sleep(5) # Allow time for connections

        initial_rafts, initial_cameras = system.get_status()
        if initial_rafts != RAFT_NODES_TO_START or initial_cameras != CAMERAS_TO_START:
             print("\nERROR: Initial system setup failed. Not all processes started correctly.")
             print(f"Expected {RAFT_NODES_TO_START} RAFT, {CAMERAS_TO_START} Cameras. Got {initial_rafts} RAFT, {initial_cameras} Cameras.")
             print("Aborting tests.")
             all_passed = False
        else:
            print("\nInitial setup complete. Starting tests...")
            # Run Tests
            for test in TEST_PLAN:
                if not system.running: # Check if Ctrl+C was pressed
                     print("Cleanup signal received, stopping tests.")
                     all_passed = False
                     break
                passed = run_test_case(test["label"], test["description"], system, test["actions"], test["check"])
                if not passed:
                    all_passed = False
                    # Decide whether to continue after failure or stop
                    # print("Stopping tests due to failure.")
                    # break

    except Exception as e:
        print(f"\nFATAL ERROR during test execution: {e}")
        all_passed = False
    finally:
        system.cleanup() # Ensure cleanup happens

    print("\n" + "="*50)
    print(" Test Suite Summary")
    print("="*50)
    if all_passed:
        print("All tests passed!")
    else:
        print("One or more tests failed or setup failed.")
    print("="*50)

if __name__ == "__main__":
    main() 