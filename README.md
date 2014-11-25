DISCLAIMER
----------
Please note: all tools/ scripts in this repo are released for use "AS IS" without any warranties of any kind, including, but not limited to their installation, use, or performance. We disclaim any and all warranties, either express or implied, including but not limited to any warranty of noninfringement, merchantability, and/ or fitness for a particular purpose. We do not warrant that the technology will meet your requirements, that the operation thereof will be uninterrupted or error-free, or that any errors will be corrected.
Any use of these scripts and tools is at your own risk. There is no guarantee that they have been through thorough testing in a comparable environment and we are not responsible for any damage or data loss incurred with their use.
You are responsible for reviewing and testing any scripts you run thoroughly before use in any non-testing environment.

# Socialite 
## Social Data Reference Architecture

A very popular MongoDB use case is the implementation of a social data status feed. While the type of data and the way in which feeds are aggregated and delivered to users varies widely, the fundamentals of these systems are relatively simple and decompose to a basic set of services. 

Socialite is a reference architecture that can be used to cover a wide range of social network use cases. The three basic goals of this reference architecture are :

1. Provide a production capable reusable framework for social data management including user graph, flexible content types and feed aggregation and delivery.
2. Demonstrate best practices for social data modelling, indexing and querying in MongoDB for implementing user graphs, content stores and feed caches.
3. Demonstrate how a flexible service architecture can be coupled with MongoDB sharding to create a scalable platform for social data.
 
## Learn More

* [Building and running](docs/building.md) Socialite
* [REST API](docs/rest.md) with Examples
* [High Level Architecture](docs/architecture.md)
* Service Detailed Design
  * [User Graph](docs/graph.md) Service
  * [Feed](docs/feed.md) Service
  * [Content](docs/content.md) Service
  * [Async](docs/async.md) Service
* Built in [benchmarking](docs/benchmarking.md)
* [Configuration reference](docs/config.md) 
* [Planned](docs/planned.md) features

