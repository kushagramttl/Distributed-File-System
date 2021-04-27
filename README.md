# DistributedStorage
**CS 6650 - Final Project Report**

The solution is divided into 4 parts:

1. Client
2. Master Server
3. Chunk Server
4. Port Discovery

Each part has a jar file

1. Client.jar- The sample command to run the client jar file is:  
java -jar Client.jar localhost 9100  
The structure of the command is:  
java -jar Client.jar <master_server_address> <server_port_number>   

2. Server.jar     
   java -Dmongodb.uri="connection_string" -jar Server.jar 9100   
   The structure of the command is:    
   java <VM_Options> -jar Server.jar <server_port_number>     
   
3. The port discovery service:   
   java -jar PortDiscovery.jar 36000 9100,9200    
   Thus, the structure for running the server is:     
   java -jar PortDiscovery.jar <port_discovery_port> <comma_separated_list_master_server_ports>     
     
 4. The Chunk.jar:     
     java -Dmongodb.uri="connection_string" -jar Chunk.jar 9091 -1 36000    
     The chunk server can be a replica chunk server or the main chunk server.     
     Thus, the command structure is:         
     java <VM_Options> -jar Chunk.jar <port_number> <main_chunk_port> <port_discovery_port>      
     If the chunk is the main chunk, the main_chunk_port = -1       

 



The chunk server can be a replica chunk server or the main chunk server.
 Thus, the command structure is:
“mongodb+srv://admin:admin@cluster0.giyol.mongodb.net/myFirstDatabase?retryWrites=true&w=majority”

If the chunk is the main chunk, the main\_chunk\_port = -1

The connection\_string needs to be- "mongodb+srv://admin:admin@cluster0.giyol.mongodb.net/myFirstDatabase?retryWrites=true&amp;w=majority"

If you face any issue while executing the jar files, please contact us and we will try our best to solve them.

Log files will be generated for each server/client that is initiated with the name including the timestamp of file creation.

To run commands on the Client side:

Type in your requests in the following format

Type in your requests in the following format   
UPLOAD: "UPLOAD,<file_name>,<file_path>"    
GET: "GET,<file_name>"    
DELETE: "DELETE,<file_name>"    
UPDATE: "UPDATE,<file_name>,<file_path>"    
quit or close to exit     
  
