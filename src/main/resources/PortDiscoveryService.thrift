namespace java server

typedef i32 int

service PortDiscoveryService{
    /**
    * port: the integer port number which the calling chunk server was started with
    * replicaPort" port of the chunk server it replicates, if equals 0, the calling chunk server is not a replica
    **/
    bool registerChunk(1:int port,2:int replicaPort),
//    bool registerMaster(1:int port),
}