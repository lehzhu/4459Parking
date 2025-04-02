import time
import grpc
from concurrent import futures
import threading
import datetime

import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

# Dictionary to store service heartbeats
# Format: {service_id: {"last_heartbeat": timestamp, "status": status}}
service_registry = {}

# Number of seconds before a service is considered dead
HEARTBEAT_TIMEOUT = 15

class ViewService(heartbeat_service_pb2_grpc.ViewServiceServicer):
    def Heartbeat(self, request, context):
        """Record a heartbeat from a service"""
        service_id = request.service_identifier
        timestamp = request.timestamp or int(time.time())
        status = request.status or "OK"
        
        # Update service registry
        service_registry[service_id] = {
            "last_heartbeat": timestamp,
            "status": status
        }
        
        print(f"Received heartbeat from {service_id} at {datetime.datetime.fromtimestamp(timestamp)}")
        return heartbeat_service_pb2.HeartbeatResponse(
            acknowledged=True,
            message="Heartbeat recorded"
        )
    
    def GetServiceStatus(self, request, context):
        """Get the status of registered services"""
        current_time = int(time.time())
        response = heartbeat_service_pb2.ServiceStatusResponse()
        
        # Filter services if identifiers are provided
        service_ids = list(request.service_identifiers) if request.service_identifiers else service_registry.keys()
        
        for service_id in service_ids:
            if service_id in service_registry:
                service_data = service_registry[service_id]
                last_heartbeat = service_data["last_heartbeat"]
                is_alive = (current_time - last_heartbeat) < HEARTBEAT_TIMEOUT
                
                status = heartbeat_service_pb2.ServiceStatus(
                    service_identifier=service_id,
                    status=service_data["status"],
                    last_heartbeat=last_heartbeat,
                    is_alive=is_alive
                )
                response.services.append(status)
        
        return response

def monitor_services():
    """Periodically check service health and print status"""
    while True:
        current_time = int(time.time())
        print("\n--- Service Health Check ---")
        for service_id, data in service_registry.items():
            last_heartbeat = data["last_heartbeat"]
            seconds_since_heartbeat = current_time - last_heartbeat
            is_alive = seconds_since_heartbeat < HEARTBEAT_TIMEOUT
            status = "ALIVE" if is_alive else "DEAD"
            print(f"{service_id}: {status} (Last heartbeat: {seconds_since_heartbeat}s ago)")
        print("---------------------------\n")
        time.sleep(10)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(ViewService(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    print("Heartbeat service listening on port 50053...")
    
    # Start monitoring thread
    monitor_thread = threading.Thread(target=monitor_services, daemon=True)
    monitor_thread.start()
    
    server.wait_for_termination()

if __name__ == '__main__':
    serve()