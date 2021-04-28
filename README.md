# DistributedStorage
**CS 6650 - Final Project Report**

The solution is divided into 4 parts:

1. Client
2. Master Server
3. Chunk Server
4. Port Discovery

We have submitted an intellij project.

The run configurations have been added in the submission.
They can be imported for setting the configuration on local system.  

 
These should be run in the following order: 
1. MasterServer 1
2. MasterServer 2
3. PortDiscovery
4. ChunkServer 1
5. ChunkServer 2
6. ChunkServer 3
7. ChunkServer Replica
8. Client


The chunk server can be a replica chunk server or the main chunk server.
 Thus, the command structure is:
“mongodb+srv://admin:admin@cluster0.giyol.mongodb.net/myFirstDatabase?retryWrites=true&w=majority”

If the chunk is the main chunk, the main\_chunk\_port = -1

The connection\_string needs to be- "mongodb+srv://admin:admin@cluster0.giyol.mongodb.net/myFirstDatabase?retryWrites=true&amp;w=majority"

If you face any issue while executing the  files, please contact us and we will try our best to solve them.

Log files will be generated for each server/client that is initiated with the name including the timestamp of file creation.

To run commands on the Client side:

Type in your requests in the following format

Type in your requests in the following format   
UPLOAD: "UPLOAD,<file_name>,<file_path>"    
GET: "GET,<file_name>"    
DELETE: "DELETE,<file_name>"    
UPDATE: "UPDATE,<file_name>,<file_path>"    
quit or close to exit     
  
