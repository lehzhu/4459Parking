# 4459Parking

Distributed Parking System with Real-Time Video Streaming

This project simulates a distributed system for tracking parking availability on a university campus. It includes both traditional update replication (primary–backup model) and a simulated real-time video stream from campus cameras. RAFT is implemented to handle server outages. 

🧱 Components

🚗 Parking System
	•	parking_primary.py: Handles write requests from clients and forwards updates to the backup.
	•	parking_backup.py: Stores replicated parking updates.
	•	parking.proto: gRPC definitions for parking update messages and services.

🎥 Video Streaming System
	•	camera_video_client.py: Simulates a camera sending a live video feed (frame-by-frame).
	•	video_processor.py: Receives video frames and processes them (e.g. to detect open spots).
	•	video_stream.proto: gRPC definitions for video streaming messages and services.

🛠 How to Run
	1.	Generate gRPC files from .proto files:

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. parking.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. video_stream.proto


	2.	Start the parking services (in separate terminals):

python parking_backup.py
python parking_primary.py


	3.	Start the video processing service:

python video_processor.py


	4.	Run the camera simulation:

python camera_video_client.py



📦 What It Simulates
	•	A campus camera sends live “video frames” to a central processing node.
	•	The processor extracts simulated data from each frame (e.g., parking availability).
	•	Parking data is replicated to a backup server for fault tolerance.

✅ Features
	•	Distributed primary–backup replication.
	•	Real-time gRPC streaming of video frames.
	•	Easy to extend with actual computer vision or ML processing.
