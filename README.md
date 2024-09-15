# high-throughput-kafka-data-streaming-service project
Author: Dan Lepham (dan.lepham@gmail.com)

Project Description: 
- A highly scalable and production-ready Kafka library that uses Kafka native apis, parallelism and concurrency to enable high-throughput message processing.
- Used to create Kubernetes cluster(s) that run multiple (or hundreds/thousands) instances of this service to process over 100+ million messages per second.
- You can create a Docker image of this service and scale it across your Kubernetes clusters as needed.
- Built on top of the powerful Quarkus Java Framework to allow extremely fast scaling across your Kubernetes clusters. 