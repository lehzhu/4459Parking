import time
import grpc
import video_stream_pb2
import video_stream_pb2_grpc

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
    channel = grpc.insecure_channel('localhost:60061')
    stub = video_stream_pb2_grpc.VideoStreamServiceStub(channel)
    # Open a bidirectional streaming connection.
    response_iterator = stub.StreamVideo(generate_frames())
    try:
        for ack in response_iterator:
            print(f"Received ack from server: {ack.message}")
    except grpc.RpcError as e:
        print(f"Stream terminated with error: {e}")

if __name__ == '__main__':
    stream_video()