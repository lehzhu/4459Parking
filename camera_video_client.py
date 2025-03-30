import time
import grpc
import video_stream_pb2
import video_stream_pb2_grpc
import random
import os
import socket

# Default server address
DEFAULT_SERVER = 'localhost:60061'

def discover_server():
    """Tries to discover the current leader in a RAFT cluster.
    Returns the leader's address or falls back to the default."""
    # List of potential server addresses in the RAFT cluster
    server_addresses = [
        'localhost:60061',  # Default server
        'localhost:60062',  # Backup server 1
        'localhost:60063',  # Backup server 2
    ]
    
    # Try each server to find the leader
    for address in server_addresses:
        try:
            with grpc.insecure_channel(address) as channel:
                # Create a stub with a short timeout
                channel = grpc.insecure_channel(
                    address,
                    options=[
                        ('grpc.enable_http_proxy', 0),
                        ('grpc.keepalive_timeout_ms', 1000),
                    ]
                )
                stub = video_stream_pb2_grpc.VideoStreamServiceStub(channel)
                
                # Try a quick ping (you'd need to implement this in your service)
                # For now, we'll just assume the first responding server is usable
                response = stub.StreamVideo(iter([generate_test_frame()]))
                next(response, None)  # Just try to get the first response
                
                print(f"Found active server at {address}")
                return address
        except grpc.RpcError:
            print(f"Server at {address} is not responding")
            continue
    
    print(f"No active servers found, using default: {DEFAULT_SERVER}")
    return DEFAULT_SERVER

def generate_test_frame():
    """Generate a test frame for server discovery."""
    return video_stream_pb2.VideoFrame(
        frame_number=0,
        data=b"test",
        timestamp=int(time.time())
    )

def generate_frames():
    frame_number = 1
    while True:
        # Simulate capturing a frame: here, 'data' is just a byte string.
        frame_data = f"Simulated frame data for frame {frame_number}".encode('utf-8')
        timestamp = int(time.time())
        
        frame = video_stream_pb2.VideoFrame(
            frame_number=frame_number,
            data=frame_data,
            timestamp=timestamp
        )
        print(f"Sending frame #{frame_number} at {timestamp}")
        yield frame
        
        frame_number += 1
        # Simulate frame rate (e.g., 1 frame per second).
        time.sleep(1)

def stream_video():
    # Discover the current leader
    server_address = discover_server()
    
    # Connect to the discovered server
    channel = grpc.insecure_channel(server_address)
    stub = video_stream_pb2_grpc.VideoStreamServiceStub(channel)
    
    # Open a bidirectional streaming connection.
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response_iterator = stub.StreamVideo(generate_frames())
            for ack in response_iterator:
                print(f"Received ack from server: {ack.message}")
                # Reset retry count on successful communication
                retry_count = 0
        except grpc.RpcError as e:
            print(f"Stream terminated with error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying in {retry_count} seconds... (Attempt {retry_count}/{max_retries})")
                time.sleep(retry_count)  # Exponential backoff
                # Rediscover the server (leader may have changed)
                server_address = discover_server()
                channel = grpc.insecure_channel(server_address)
                stub = video_stream_pb2_grpc.VideoStreamServiceStub(channel)
            else:
                print("Max retries reached. Giving up.")
                break

if __name__ == '__main__':
    stream_video()