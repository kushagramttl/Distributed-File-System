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
java -jar Client.jar \&lt;master\_server\_address\&gt; \&lt;server\_port\_number\&gt;
2. Server.jar
java -Dmongodb.uri=&quot;connection\_string&quot; -jar Server.jar 9100
The structure of the command is:
java \&lt;VM\_Options\&gt; -jar Server.jar \&lt;server\_port\_number\&gt;
3. The port discovery service:
java -jar PortDiscovery.jar 36000 9100,9200
 Thus, the structure for running the server is:
java -jar PortDiscovery.jar \&lt;port\_discovery\_port\&gt; \&lt;comma\_separated\_list\_master\_server\_ports\&gt;
4. The Chunk.jar:
java -Dmongodb.uri=&quot;connection\_string&quot; -jar Chunk.jar 9091 -1 36000

The chunk server can be a replica chunk server or the main chunk server.
 Thus, the command structure is:
java \&lt;VM\_Options\&gt; -jar Chunk.jar \&lt;port\_number\&gt; \&lt;main\_chunk\_port\&gt; \&lt;port\_discovery\_port\&gt;

If the chunk is the main chunk, the main\_chunk\_port = -1

The connection\_string needs to be- &quot;mongodb+srv://admin:admin@cluster0.giyol.mongodb.net/myFirstDatabase?retryWrites=true&amp;w=majority&quot;

If you face any issue while executing the jar files, please contact us and we will try our best to solve them.

Log files will be generated for each server/client that is initiated with the name including the timestamp of file creation.

To run commands on the Client side:

Type in your requests in the following format

UPLOAD: &quot;UPLOAD,\&lt;file\_name\&gt;,\&lt;file\_path\&gt;&quot;

GET: &quot;GET,\&lt;file\_name\&gt;&quot;

DELETE: &quot;DELETE,\&lt;file\_name\&gt;&quot;

UPDATE: &quot;UPDATE,\&lt;file\_name\&gt;,\&lt;file\_path\&gt;&quot;

quit or close to exit
