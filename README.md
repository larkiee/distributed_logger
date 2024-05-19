A microservice blueprint with gRpc written in GO  ![go logo](https://go.dev/blog/go-brand/Go-Logo/PNG/Go-Logo_Aqua.png)
------
the purpose of this repository is to outline a guide for people that want to write distributed microservice in golang

### Intention
this repo developed for outling a structure for writting gRpc based microservise that interact with other microservices so its api does'nt expose with json data API.

### Main packages
* log: the main logic of web server is implemented in this package and can be ignored
* server: this package contains server implementaion of log service type of gRpc
* discovery: this package contain service discovery logic that implemented by [serf](https://github.com/hashicorp/serf) package
* api: contain protobuf definition compiled code by protoc compiler