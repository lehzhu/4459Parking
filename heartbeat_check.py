import grpc
import argparse
import time
import sys
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

def check_services(service_ids=None):
    """Check the status of services registered with the heartbeat service"""
    channel = grpc.insecure_channel('localhost:50053')
    stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
    
    request = heartbeat_service_pb2.ServiceStatusRequest()
    if service_ids:
        for service_id in service_ids:
            request.service_identifiers.append(service_id)
    
    try:
        response = stub.GetServiceStatus(request)
        
        print("\n--- Service Status ---")
        if not response.services:
            print("No services registered")
        else:
            for service in response.services:
                status = "ALIVE" if service.is_alive else "DEAD"
                last_heartbeat_time = time.strftime('%Y-%m-%d %H:%M:%S', 
                                                   time.localtime(service.last_heartbeat))
                print(f"Service: {service.service_identifier}")
                print(f"  Status: {status}")
                print(f"  Last Heartbeat: {last_heartbeat_time}")
                print(f"  Service Status: {service.status}")
                print("--------------------")
        return True
    except grpc.RpcError as e:
        print(f"Error connecting to heartbeat service: {e}")
        return False

def monitor_mode(interval=5):
    """Continuously monitor services"""
    print(f"Monitoring services every {interval} seconds. Press Ctrl+C to exit.")
    try:
        while True:
            check_services()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nExiting monitor mode.")

def main():
    parser = argparse.ArgumentParser(description="Check status of distributed services")
    parser.add_argument("--services", "-s", nargs="*", help="Specific service IDs to check")
    parser.add_argument("--monitor", "-m", action="store_true", help="Monitor mode - continuously check services")
    parser.add_argument("--interval", "-i", type=int, default=5, help="Monitoring interval in seconds (default: 5)")
    
    args = parser.parse_args()
    
    if args.monitor:
        monitor_mode(args.interval)
    else:
        if not check_services(args.services):
            sys.exit(1)

if __name__ == "__main__":
    main()