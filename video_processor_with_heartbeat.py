import time
import grpc
from concurrent import futures
import threading

import video_stream_pb2
import video_stream_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

class VideoStreamService(video_stream_pb2_grpc.VideoStreamServiceServicer):
    def StreamVideo(self, request_iterator, context):
        # For each frame received from the camera client...
        for frame in request_iterator:
            # Simulate processing: here you might analyze the frame for parking spot data.
            print(f"Processing frame #{frame.frame_number} at {frame.timestamp}")
            
            # Optionally, simulate a processing delay.
            time.sleep(0.1)
            
            # Send an acknowledgment back to the client.
            ack = video_stream_pb2.VideoAck(message=f"Frame {frame.frame_number} received")
            yield ack

def send_heartbeats():
    """Send periodic heartbeats to the view service"""
    channel = grpc.insecure_channel('localhost:50053')
    stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
    
    while True:
        try:
            heartbeat_request = heartbeat_service_pb2.HeartbeatRequest(
                service_identifier="video_processor",
                status="PROCESSING"  # or any other status you want to report
            )
            response = stub.Heartbeat(heartbeat_request)
            if response.acknowledged:
                print("Heartbeat acknowledged")
            else:
                print(f"Heartbeat issue: {response.message}")
        except Exception as e:
            print(f"Failed to send heartbeat: {e}")
        
        time.sleep(5)  # Send heartbeat every 5 seconds

def serve():
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
    heartbeat_thread.start()
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    video_stream_pb2_grpc.add_VideoStreamServiceServicer_to_server(VideoStreamService(), server)
    server.add_insecure_port('[::]:60061')
    server.start()
    print("Video Processor server listening on port 60061...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()