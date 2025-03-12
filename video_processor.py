import time
import grpc
from concurrent import futures

import video_stream_pb2
import video_stream_pb2_grpc

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

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    video_stream_pb2_grpc.add_VideoStreamServiceServicer_to_server(VideoStreamService(), server)
    server.add_insecure_port('[::]:60061')
    server.start()
    print("Video Processor server listening on port 60061...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()