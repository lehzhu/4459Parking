import time
import grpc
import asyncio
import threading
from concurrent import futures
import argparse
import os
import sys
import random
import hashlib
from collections import defaultdict

import video_stream_pb2
import video_stream_pb2_grpc

# Import RAFT proto definitions
# Note: You'll need to generate these first with:
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
import raft_pb2
import raft_pb2_grpc

# State enum for the RAFT protocol
class RaftState:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

class RaftVideoProcessor(raft_pb2_grpc.RaftServicer):
    """A video processor that uses the RAFT consensus algorithm for fault tolerance."""
    
    def __init__(self, node_id, port, peer_addresses):
        self.node_id = node_id
        self.port = port
        self.peer_addresses = peer_addresses
        
        # RAFT state
        self.state = RaftState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state (reinitialized after election)
        self.next_index = {}
        self.match_index = {}
        
        # Timing parameters
        self.election_timeout_min = 150  # milliseconds
        self.election_timeout_max = 300  # milliseconds
        self.heartbeat_interval = 50     # milliseconds
        
        # Set up heartbeat timer
        self.reset_election_timer()
        
        # Set up video processing state
        self.frames_processed = 0
        self.last_processed_frame = None
        self.camera_stats = defaultdict(lambda: {'last_seen': 0, 'frames_processed': 0})
        
        # Create stubs to communicate with peers
        self.peer_stubs = {}
        self.initialize_peer_stubs()
    
    def initialize_peer_stubs(self):
        """Initialize gRPC stubs for all peers."""
        for peer_id, peer_address in self.peer_addresses.items():
            if peer_id == self.node_id:
                continue
            
            try:
                channel = grpc.insecure_channel(peer_address)
                self.peer_stubs[peer_id] = raft_pb2_grpc.RaftStub(channel)
            except Exception as e:
                print(f"Error creating stub for peer {peer_id}: {e}")
    
    def reset_election_timer(self):
        """Reset the election timeout timer with a random value."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()
        
        # Random timeout value
        timeout = (self.election_timeout_min + 
                  (self.election_timeout_max - self.election_timeout_min) * 
                  random.random()) / 1000.0
        
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()
    
    def start_election(self):
        """Start an election to become the leader."""
        if self.state == RaftState.LEADER:
            # We're already the leader, restart timer
            self.reset_election_timer()
            return
        
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        
        # We vote for ourselves
        votes_received = 1
        
        print(f"Starting election for term {self.current_term}")
        
        # Request votes from all peers
        for peer_id, stub in self.peer_stubs.items():
            try:
                # Create RequestVote request
                request = raft_pb2.RequestVoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id,
                    last_log_index=len(self.log),
                    last_log_term=self.log[-1].term if self.log else 0
                )
                
                # Send RequestVote RPC
                response = stub.RequestVote(request, timeout=1.0)
                
                # Count vote if granted
                if response.vote_granted:
                    votes_received += 1
                    print(f"Received vote from peer {peer_id}")
                
                # If peer has higher term, revert to follower
                if response.term > self.current_term:
                    print(f"Discovered higher term from peer {peer_id}, reverting to follower")
                    self.current_term = response.term
                    self.state = RaftState.FOLLOWER
                    self.voted_for = None
                    self.reset_election_timer()
                    return
                
            except Exception as e:
                print(f"Error requesting vote from peer {peer_id}: {e}")
        
        # If we got majority of votes, become leader
        if votes_received > len(self.peer_addresses) / 2:
            self.become_leader()
        else:
            # If we didn't get enough votes, go back to follower state
            self.state = RaftState.FOLLOWER
            self.reset_election_timer()
    
    def become_leader(self):
        """Transition to leader state."""
        if self.state == RaftState.CANDIDATE:
            self.state = RaftState.LEADER
            print(f"Became leader for term {self.current_term}")
            
            # Initialize leader state
            for peer_id in self.peer_addresses:
                if peer_id != self.node_id:
                    self.next_index[peer_id] = len(self.log) + 1
                    self.match_index[peer_id] = 0
            
            # Start sending heartbeats
            self.send_heartbeats()
    
    def send_heartbeats(self):
        """Send heartbeats to all followers to maintain leadership."""
        if self.state != RaftState.LEADER:
            return
        
        for peer_id, stub in self.peer_stubs.items():
            try:
                # Create empty AppendEntries request (heartbeat)
                prev_log_index = self.next_index.get(peer_id, 1) - 1
                prev_log_term = 0
                if prev_log_index > 0 and prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1].term
                
                request = raft_pb2.AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=[],  # Empty for heartbeat
                    leader_commit=self.commit_index
                )
                
                # Send AppendEntries RPC
                response = stub.AppendEntries(request, timeout=1.0)
                
                # Handle response
                if response.term > self.current_term:
                    print(f"Discovered higher term from peer {peer_id}, reverting to follower")
                    self.current_term = response.term
                    self.state = RaftState.FOLLOWER
                    self.voted_for = None
                    self.reset_election_timer()
                    return
                
            except Exception as e:
                print(f"Error sending heartbeat to peer {peer_id}: {e}")
        
        # Schedule next heartbeat
        heartbeat_timer = threading.Timer(self.heartbeat_interval / 1000.0, self.send_heartbeats)
        heartbeat_timer.daemon = True
        heartbeat_timer.start()
    
    def calculate_parking_spaces(self, frame_data):
        """Calculate parking spaces from frame data using a hash function."""
        # Use SHA-256 to generate a deterministic hash from the frame data
        # This is a substitute for the real vision processing.
        hash_value = hashlib.sha256(frame_data).hexdigest()
        
        # Convert the first 4 characters of the hash to an integer
        # This gives us a number between 0 and 65535 (16^4)
        spaces_hash = int(hash_value[:4], 16)
        
        # Normalize to a reasonable number of parking spaces (0-100)
        parking_spaces = spaces_hash % 101
        
        return parking_spaces

    def process_frame(self, frame):
        """Process a video frame, updating the distributed log."""
        self.frames_processed += 1
        self.last_processed_frame = frame
        
        # Update camera statistics
        camera_id = frame.camera_id
        self.camera_stats[camera_id]['last_seen'] = time.time()
        self.camera_stats[camera_id]['frames_processed'] += 1
        
        # Calculate parking spaces
        parking_spaces = self.calculate_parking_spaces(frame.data)
        
        # If we're the leader, append to log and replicate
        if self.state == RaftState.LEADER:
            # Create log entry with parking spaces calculation
            log_entry = raft_pb2.LogEntry(
                index=len(self.log) + 1,
                term=self.current_term,
                command=frame.data,
                camera_id=camera_id,
                parking_spaces=parking_spaces,
                timestamp=frame.timestamp
            )
            self.log.append(log_entry)
            
            # Replicate to followers
            self.replicate_log_to_followers()
            
            # Clearer logging for the leader
            print(f"[LEADER {self.node_id}] Processed Frame #{frame.frame_number} from Camera {camera_id} -> Parking Spaces: {parking_spaces}")
            return True
        else:
            # If we're not the leader, forward to leader
            self.forward_frame_to_leader(frame)
            return False
    
    def replicate_log_to_followers(self):
        """Replicate log entries to followers."""
        if self.state != RaftState.LEADER:
            return
            
        for peer_id, stub in self.peer_stubs.items():
            try:
                # Determine entries to send (based on nextIndex)
                next_idx = self.next_index.get(peer_id, 1)
                entries_to_send = []
                
                if next_idx <= len(self.log):
                    entries_to_send = self.log[next_idx - 1:]
                
                # Skip if no entries to send
                if not entries_to_send:
                    continue
                
                # Create AppendEntries request
                prev_log_index = next_idx - 1
                prev_log_term = 0
                if prev_log_index > 0 and prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1].term
                
                request = raft_pb2.AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries_to_send,
                    leader_commit=self.commit_index
                )
                
                # Send AppendEntries RPC
                response = stub.AppendEntries(request, timeout=1.0)
                
                # Handle response
                if response.success:
                    # Update match index and next index
                    self.match_index[peer_id] = prev_log_index + len(entries_to_send)
                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                    print(f"Successfully replicated log to peer {peer_id}")
                else:
                    # Decrement next index and retry
                    self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
                    print(f"Failed to replicate log to peer {peer_id}, will retry with earlier entries")
                
                if response.term > self.current_term:
                    print(f"Discovered higher term from peer {peer_id}, reverting to follower")
                    self.current_term = response.term
                    self.state = RaftState.FOLLOWER
                    self.voted_for = None
                    self.reset_election_timer()
                    return
                
            except Exception as e:
                print(f"Error replicating log to peer {peer_id}: {e}")
    
    def forward_frame_to_leader(self, frame):
        """Forward a frame to the current leader."""
        if self.state == RaftState.LEADER:
            return  # We are the leader, no need to forward
            
        # Find leader
        leader_id = None
        for peer_id, peer_address in self.peer_addresses.items():
            if peer_id != self.node_id:
                try:
                    # Just try to ask each peer for a heartbeat
                    stub = self.peer_stubs[peer_id]
                    leader_id = peer_id
                    break
                except Exception:
                    continue
        
        if leader_id is None:
            print("No leader found to forward frame to")
            return
            
        try:
            # Forward the frame
            stub = self.peer_stubs[leader_id]
            
            # Create forward request
            request = raft_pb2.VideoFrameForward(
                term=self.current_term,
                sender_id=self.node_id,
                frame_number=frame.frame_number,
                data=frame.data,
                timestamp=frame.timestamp
            )
            
            # Send ForwardVideoFrame RPC
            response = stub.ForwardVideoFrame(request, timeout=1.0)
            
            if response.success:
                print(f"Successfully forwarded frame #{frame.frame_number} to leader {leader_id}")
            else:
                print(f"Failed to forward frame #{frame.frame_number} to leader {leader_id}: {response.message}")
            
            # Update term if needed
            if response.term > self.current_term:
                self.current_term = response.term
                self.voted_for = None
                
        except Exception as e:
            print(f"Error forwarding frame to leader {leader_id}: {e}")
    
    def monitor_cameras(self):
        """Monitor camera health and handle sudden influx."""
        while True:
            current_time = time.time()
            active_cameras = 0
            
            # Check each camera's status
            for camera_id, stats in self.camera_stats.items():
                # If camera hasn't been seen in 30 seconds, consider it inactive
                if current_time - stats['last_seen'] > 30:
                    print(f"Camera {camera_id} appears to be inactive")
                else:
                    active_cameras += 1
            
            print(f"Currently monitoring {active_cameras} active cameras")
            time.sleep(10)  # Check every 10 seconds

    def start_monitoring(self):
        """Start the camera monitoring thread."""
        monitor_thread = threading.Thread(target=self.monitor_cameras)
        monitor_thread.daemon = True
        monitor_thread.start()
    
    # RAFT RPC implementations
    
    def RequestVote(self, request, context):
        """Implement RequestVote RPC."""
        # Reset election timer since we heard from someone
        self.reset_election_timer()
        
        # If request term is less than our term, reject
        if request.term < self.current_term:
            return raft_pb2.RequestVoteResponse(
                term=self.current_term,
                vote_granted=False
            )
        
        # If request term is greater than our term, update term and become follower
        if request.term > self.current_term:
            self.current_term = request.term
            self.state = RaftState.FOLLOWER
            self.voted_for = None
        
        # Check if we can vote for this candidate
        can_vote = (self.voted_for is None or self.voted_for == request.candidate_id)
        
        # Check if candidate's log is at least as up-to-date as ours
        last_log_index = len(self.log)
        last_log_term = 0
        if last_log_index > 0:
            last_log_term = self.log[last_log_index - 1].term
            
        log_ok = (request.last_log_term > last_log_term or 
                 (request.last_log_term == last_log_term and 
                  request.last_log_index >= last_log_index))
        
        # Grant vote if conditions are met
        vote_granted = can_vote and log_ok
        
        if vote_granted:
            self.voted_for = request.candidate_id
            print(f"Granted vote to candidate {request.candidate_id} for term {request.term}")
        
        return raft_pb2.RequestVoteResponse(
            term=self.current_term,
            vote_granted=vote_granted
        )
    
    def AppendEntries(self, request, context):
        """Implement AppendEntries RPC."""
        # Reset election timer since we heard from a leader
        self.reset_election_timer()
        
        # If request term is less than our term, reject
        if request.term < self.current_term:
            return raft_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=False
            )
        
        # If request term is greater than or equal to our term, 
        # accept the leader and update term if needed
        if request.term >= self.current_term:
            self.current_term = request.term
            self.state = RaftState.FOLLOWER
            self.voted_for = None
        
        # Check if previous log entry matches
        prev_log_index = request.prev_log_index
        if prev_log_index > 0:
            if prev_log_index > len(self.log):
                # We don't have the previous entry
                return raft_pb2.AppendEntriesResponse(
                    term=self.current_term,
                    success=False
                )
            
            if prev_log_index > 0 and self.log[prev_log_index - 1].term != request.prev_log_term:
                # Previous entry term doesn't match
                return raft_pb2.AppendEntriesResponse(
                    term=self.current_term,
                    success=False
                )
        
        # Process entries
        if request.entries:
            # Delete conflicting entries and add new ones
            for i, entry in enumerate(request.entries):
                if prev_log_index + i + 1 <= len(self.log):
                    if self.log[prev_log_index + i].term != entry.term:
                        # Delete conflicting entry and all that follow
                        self.log = self.log[:prev_log_index + i]
                        break
                else:
                    # Append new entries
                    self.log.extend(request.entries[i:])
                    break
            
            print(f"Appended {len(request.entries)} entries to log")
        
        # Update commit index
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))
            print(f"Updated commit index to {self.commit_index}")
        
        return raft_pb2.AppendEntriesResponse(
            term=self.current_term,
            success=True
        )
    
    def ForwardVideoFrame(self, request, context):
        """Handle forwarded video frames from followers."""
        # Check if we are the leader
        if self.state != RaftState.LEADER:
            return raft_pb2.ForwardResponse(
                term=self.current_term,
                success=False,
                message="Not the leader"
            )
        
        try:
            # Create a video frame from the forwarded data
            frame = video_stream_pb2.VideoFrame(
                frame_number=request.frame_number,
                data=request.data,
                timestamp=request.timestamp
            )
            
            # Process the frame
            success = self.process_frame(frame)
            
            return raft_pb2.ForwardResponse(
                term=self.current_term,
                success=success,
                message="Frame processed by leader" if success else "Failed to process frame"
            )
            
        except Exception as e:
            print(f"Error processing forwarded frame: {e}")
            return raft_pb2.ForwardResponse(
                term=self.current_term,
                success=False,
                message=f"Error: {str(e)}"
            )


class VideoStreamService(video_stream_pb2_grpc.VideoStreamServiceServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node
    
    def StreamVideo(self, request_iterator, context):
        """Handle streaming video from clients."""
        for frame in request_iterator:
            # Process the frame
            print(f"Processing frame #{frame.frame_number} at {frame.timestamp}")
            
            # Let the RAFT node process it
            success = self.raft_node.process_frame(frame)
            
            # Simulate processing delay
            time.sleep(0.1)
            
            # Send an acknowledgment back to the client
            message = "Frame processed successfully"
            if not success:
                message = "Frame received, forwarded to leader"
                
            ack = video_stream_pb2.VideoAck(message=message)
            yield ack


def serve(node_id, port, peer_addresses):
    """Start the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Create the RAFT node
    raft_node = RaftVideoProcessor(node_id, port, peer_addresses)
    
    # Register the RAFT service
    raft_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    
    # Register the video stream service
    service = VideoStreamService(raft_node)
    video_stream_pb2_grpc.add_VideoStreamServiceServicer_to_server(service, server)
    
    # Start the server
    server_address = f'[::]:{port}'
    server.add_insecure_port(server_address)
    server.start()
    
    print(f"Video processor (node {node_id}) listening on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Server stopping...")
    finally:
        if hasattr(raft_node, 'election_timer'):
            raft_node.election_timer.cancel()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start a RAFT video processor node')
    parser.add_argument('--id', type=int, required=True, help='Node ID')
    parser.add_argument('--port', type=int, required=True, help='Port to listen on')
    parser.add_argument('--peers', type=str, required=True, 
                        help='Comma-separated list of peer addresses (format: id:host:port,...)')
    
    args = parser.parse_args()
    
    # Parse peer addresses
    peer_addresses = {}
    for peer in args.peers.split(','):
        if not peer:
            continue
        parts = peer.split(':')
        if len(parts) >= 3:
            peer_id = int(parts[0])
            peer_host = parts[1]
            peer_port = int(parts[2])
            peer_addresses[peer_id] = f"{peer_host}:{peer_port}"
    
    # Add self to peer addresses
    peer_addresses[args.id] = f"localhost:{args.port}"
    
    serve(args.id, args.port, peer_addresses) 