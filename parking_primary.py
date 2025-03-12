import grpc
from concurrent import futures
import time
import threading

import parking_pb2
import parking_pb2_grpc

import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from google.protobuf.empty_pb2 import Empty

# Store the latest valid parking update per camera.
primary_store = {}

class ParkingPrimaryService(parking_pb2_grpc.ParkingServiceServicer):
    def __init__(self, backup_stub):
        self.backup_stub = backup_stub

    def Update(self, request, context):
        ack_message = ""
        # Process the update from the camera.
        if request.status != "OK":
            # Fallback: if the update is in error, use last known good data.
            print(f"Received error from camera {request.camera_id}. Checking fallback.")
            if request.camera_id in primary_store:
                fallback_update = primary_store[request.camera_id]
                ack_message = f"Fallback used. Last known data: {fallback_update}"
            else:
                ack_message = "Error received and no fallback available."
                print(f"No fallback available for camera {request.camera_id}.")
        else:
            # If update is valid, update the primary store.
            primary_store[request.camera_id] = {
                "location": request.location,
                "available_spaces": request.available_spaces,
                "timestamp": request.timestamp
            }
            ack_message = "Primary ACK"

        # Forward the update to the backup server.
        try:
            backup_response = self.backup_stub.Update(request)
            if backup_response.ack == "Backup ACK":
                with open("parking_primary.txt", "a") as f:
                    f.write(f"{request.camera_id} {request.location} {request.available_spaces} "
                            f"{request.status} {request.timestamp}\n")
            else:
                print("Backup did not acknowledge update properly.")
                ack_message += " | Backup failed"
        except Exception as e:
            print(f"Error updating backup: {e}")
            ack_message += " | Backup error"
        
        return parking_pb2.ParkingUpdateResponse(ack=ack_message)

def send_heartbeats():
    channel = grpc.insecure_channel('localhost:50053')
    stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
    while True:
        heartbeat_request = heartbeat_service_pb2.HeartbeatRequest(
            service_identifier="parking_primary"
        )
        stub.Heartbeat(heartbeat_request)
        time.sleep(5)

def serve():
    # Start heartbeat thread.
    heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
    heartbeat_thread.start()

    # Connect to the backup server on its designated port.
    backup_channel = grpc.insecure_channel('localhost:50062')
    backup_stub = parking_pb2_grpc.ParkingServiceStub(backup_channel)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    parking_pb2_grpc.add_ParkingServiceServicer_to_server(ParkingPrimaryService(backup_stub), server)
    # Primary listens on a dedicated port.
    server.add_insecure_port('[::]:50061')
    server.start()
    print("Parking Primary server listening on port 50061...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()