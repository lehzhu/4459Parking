import grpc
from concurrent import futures
import time
import threading

import parking_pb2
import parking_pb2_grpc

# Heartbeat modules (reuse your existing heartbeat service modules)
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from google.protobuf.empty_pb2 import Empty

# Backup store for parking updates (indexed by camera_id)
backup_store = {}

class ParkingBackupService(parking_pb2_grpc.ParkingServiceServicer):
    def Update(self, request, context):
        key = request.camera_id
        # Append this update to a list for the camera.
        if key not in backup_store:
            backup_store[key] = []
        backup_store[key].append({
            "location": request.location,
            "available_spaces": request.available_spaces,
            "status": request.status,
            "timestamp": request.timestamp
        })
        # Persist the update to a backup file.
        with open("parking_backup.txt", "a") as f:
            f.write(f"{request.camera_id} {request.location} {request.available_spaces} "
                    f"{request.status} {request.timestamp}\n")
        return parking_pb2.ParkingUpdateResponse(ack="Backup ACK")

def send_heartbeats():
    channel = grpc.insecure_channel('localhost:50053')
    stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
    while True:
        heartbeat_request = heartbeat_service_pb2.HeartbeatRequest(
            service_identifier="parking_backup"
        )
        stub.Heartbeat(heartbeat_request)
        time.sleep(5)

def serve():
    # Start a heartbeat thread.
    heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
    heartbeat_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    parking_pb2_grpc.add_ParkingServiceServicer_to_server(ParkingBackupService(), server)
    # Use a dedicated port for the backup server.
    server.add_insecure_port('[::]:50062')
    server.start()
    print("Parking Backup server listening on port 50062...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()