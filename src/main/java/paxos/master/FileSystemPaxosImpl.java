package paxos.master;

import Helper.Logger;
import chunk.Chunk;
import com.mongodb.client.*;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.bson.Document;
import org.bson.types.ObjectId;
import server.FileSystem;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;


class variableCollection {
    public boolean proposal_accepted = false;
    public double accepted_ID = -1;
    public String accepted_VALUE = null;
    public int countPromises = 0;
    public int countAccepts = 0;
}


/**
 *
 */
public class FileSystemPaxosImpl implements FileSystemPaxos.Iface{


    /**
     * Variables related to paxos prepare, accept and learn
     */
    private double maxRoundIdentifier;
    private int majority;
    private Map<Double, variableCollection> trackerObject;
    private List<FileSystemPaxos.Client> paxosServers;
    private int port;



    /**
     * The logger object.
     */
    private Logger logger;

    /**
     * The client that connects to the chunk server.
     */
    private Chunk.Client chunkClient;

    private String hostName;

    /**
     *  fileLocator keeps track of file names and the port number of which chunk they reside in
     *  loadTracker keeps track of chunk port numbers and the amount of files the chunks contain.
     *  replicaTracker keeps track of each chunk's active replicas.
     */
        private Map<String, Integer> fileLocator;
        private Map<Integer, Integer> loadTracker;
        private Map<Integer, Set<Integer>> replicaTracker;

    /**
     * Creates a new instance of the FileSystemImpl
     *
     * @param logger The logger for logging.
     */
    public FileSystemPaxosImpl(Logger logger) {
        this.logger = logger;
        this.fileLocator = new ConcurrentHashMap<>();
        this.loadTracker = new ConcurrentHashMap<>();
        this.replicaTracker = new ConcurrentHashMap<>();
        retrieveMetadata();
    }

    public FileSystemPaxosImpl(Logger logger, List<ServerIdentifier> servers) {

        // Paxos Variables
        this.maxRoundIdentifier = 1 + Math.random();
        this.majority = servers.size()/2 + 1;
        this.trackerObject = new HashMap<Double, variableCollection>();
        this.paxosServers = getServers(servers);


        // Normal Variables
        this.logger = logger;
        this.fileLocator = new ConcurrentHashMap<>();
        this.loadTracker = new ConcurrentHashMap<>();
        this.replicaTracker = new ConcurrentHashMap<>();
        retrieveMetadata();

    }

    private List<FileSystemPaxos.Client> getServers(List<ServerIdentifier> paxosServerIdentifiers) {

        getConfirmation();
        List<FileSystemPaxos.Client> clients = new ArrayList<FileSystemPaxos.Client>();

        for(int i = 0; i < paxosServerIdentifiers.size(); i++) {
            if(paxosServerIdentifiers.get(i).getPort() != port) {
                FileSystemPaxos.Client paxosClient = null;
                ServerIdentifier participant = paxosServerIdentifiers.get(i);
                try {
                    String host = participant.getHostname();
                    int port = participant.getPort();
                    logger.logInfo("CONNECTING TO THE MASTER SERVER " + host + " " + port);
                    TSocket transport = new TSocket(host, port);
                    transport.open();
                    TBinaryProtocol protocol = new TBinaryProtocol(transport);
                    paxosClient = new FileSystemPaxos.Client(protocol);
                    logger.logInfo("CONNECTED TO THE MASTER SERVER " + host + " " + port);
                    clients.add(paxosClient);
                }
                catch (TTransportException e) {
                    logger.logInfo("ONE FELLOW SERVER GOT DISCONNECTED");
                    e.printStackTrace();
                }

            }
        }

        logger.logInfo("NUMBER OF MASTER SERVERS CONNECTED " + clients.size());

        return clients;
    }

    private void getConfirmation() {
        logger.logInfo("THIS SERVER WILL NOW ESTABLISH CONNECTION TO THE REST OF THEM");
        logger.logInfo("MAKE SURE THEY ALL DISPLAY THE SAME MESSAGE");
        String confirmation = "";
        Scanner sc = new Scanner(System.in);
        while (!confirmation.equals("READY")) {
            logger.logInfo("TYPE IN 'READY' TO PROCEED");
            confirmation = sc.next();
        }
    }


    // TODO IMPORTANT : CATCH EXCEPTIONS HERE

    private void retrieveMetadata(){
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> collection = db.getCollection("Metadata");
            FindIterable<Document> docs = collection.find();

            for (Document doc : docs) {
                fileLocator.put((String) doc.get("filename"), (Integer) doc.get("chunk"));
                loadTracker.put((Integer) doc.get("chunk"), (Integer) doc.get("chunk size"));
                replicaTracker.put((Integer) doc.get("chunk"), new HashSet<>());
            }
        }
    }


    /**
     * Method used to unmarshal the data related to the servers and create objects from it
     * @param paxosServerIdentifiers
     * @return
     */



    /**
     * Backs up both the data of the fileLocator and loadTracker maps in the database
     */
    private void backupMetadata(){
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            db.getCollection("Metadata").drop();
            db.createCollection("Metadata");
            MongoCollection<Document> collection = db.getCollection("Metadata");

            for (Map.Entry entry : fileLocator.entrySet()) {
                Document data = new Document("_id", new ObjectId());
                data.append("filename", entry.getKey())
                    .append("chunk", entry.getValue())
                    .append("chunk size", loadTracker.get(entry.getValue())) ;
                collection.insertOne(data);
            }
        }
    }


    @Override
    public ByteBuffer getFile(String name) throws TException {
        if (!fileLocator.containsKey(name)){
            throw new TApplicationException("File not found");
        }
        int chunkPort = fileLocator.get(name);

        // TODO: MAKE THIS CHECK ALL PORTS UNTIL ONE WORKS AND PUT IT IN A SEPARATE METHOD
        try (TTransport transport = new TSocket("localhost", chunkPort)) {
            transport.open();
        }catch(TTransportException e){
            logger.logInfo("WARNING - CHUNKSERV => " + chunkPort + " is not responding, contacting replica...");
            if (replicaTracker.get(chunkPort).isEmpty())
                throw new TApplicationException("No copy of the requested data has been found");
            chunkPort = replicaTracker.get(chunkPort).stream().findFirst().get();
        }

        try (TTransport transport = new TSocket("localhost", chunkPort)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            ByteBuffer buffer = chunkClient.getFile(name);
            logger.logInfo("REQUEST - GET; CHUNKSERV => " + chunkPort + " - FILE => " + name + " - SIZE => " + buffer.array().length * 1000 + " KB");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));


            return buffer;
        }
    }

    @Override
    public void uploadFile(String name, ByteBuffer file) throws TException {

        Map.Entry<Integer, Integer> chunkServer = Collections.min(loadTracker.entrySet(),
                new Comparator<Map.Entry<Integer, Integer>>() {
                    public int compare(Map.Entry<Integer, Integer> entry1, Map.Entry<Integer, Integer> entry2) {
                        return entry1.getValue().compareTo(entry2.getValue());
                    }
                }
        );

        /*********************************************************************************************************/

        logger.logInfo("REQUEST - UPLOAD; CHUNKSERV => " + chunkServer.getKey() + " - FILE => " + name + " - SIZE => " + file.array().length * 1000 + " KB");
        loadTracker.replace(chunkServer.getKey(), chunkServer.getValue() + 1);
        fileLocator.put(name, chunkServer.getKey());

        /***********************************************************************************************************/

        try (TTransport transport = new TSocket("localhost", chunkServer.getKey())) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);
            chunkClient.uploadFile(name, file);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));


            backupMetadata();
        }
    }

    @Override
    public void updateFile(String name, ByteBuffer file) throws TException {
        if (!fileLocator.containsKey(name)){
            throw new TApplicationException("File not found");
        }
        int chunkPort = fileLocator.get(name);

        try (TTransport transport = new TSocket("localhost", chunkPort)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            chunkClient.updateFile(name, file);

            logger.logInfo("REQUEST - UPDATE; CHUNKSERV => " + chunkPort + " - FILE => " + name + " - SIZE => " + file.array().length * 1000 + " KB");


            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
        }
    }

    @Override
    public void deleteFile(String name) throws TException {
        if (!fileLocator.containsKey(name)){
            throw new TApplicationException("File not found");
        }


        /***************************************************************************/
        int chunkPort = fileLocator.get(name);

        loadTracker.replace(chunkPort, loadTracker.get(chunkPort) - 1);
        fileLocator.remove(name);


        /***************************************************************************/

        try (TTransport transport = new TSocket("localhost", chunkPort)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            chunkClient.deleteFile(name);
            logger.logInfo("REQUEST - DELETE; CHUNKSERV => " + chunkPort + " - FILE => " + name);

            loadTracker.replace(chunkPort, loadTracker.get(chunkPort) - 1);
            fileLocator.remove(name);
            backupMetadata();


            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
        }
    }

    @Override
    public boolean registerChunk(int port, int replicaPort) throws TException {
        boolean toReturn = false;
        if (replicaPort == -1){ // Not a replica
            if (!loadTracker.containsKey(port) && !replicaTracker.containsKey(port)) {
                loadTracker.put(port, 0);
                replicaTracker.put(port, new HashSet<>());
                toReturn = true;
                logger.logInfo("REQUEST - REGISTER CHUNK; PORT => " + port);
            }else
                logger.logInfo("WARNING - tried to register already registered chunk port");
        }else{ // is a replica
            if (replicaTracker.containsKey(replicaPort)) {
                toReturn = replicaTracker.get(replicaPort).add(port);
                if (!toReturn)
                    logger.logInfo("WARNING - tried to register already registered chunk replica port");
                else
                    logger.logInfo("REQUEST - REGISTER CHUNK; PORT => " + port + " REPLICATING => " + replicaPort);
            }
        }
        return toReturn;
    }



    private void proposerHelper (String message) {

        double paxosIdentifier = maxRoundIdentifier + 1 + Math.random();

        // Map needed to record the state for each paxosIdentifier
        trackerObject.put(paxosIdentifier, new variableCollection());
        logger.logInfo("TRACKER OBJECT CREATED " + trackerObject.toString());
        logger.logInfo("CURRENT maxRoundIdentifier" + " " + maxRoundIdentifier);
        logger.logInfo("CONSENSUS paxosIdentifier" + " " + paxosIdentifier);

        // Prepare //
        // Call to the local acceptor for prepare
        logger.logInfo("UPDATING VALUES FOR THE LOCAL PROPOSER");
        String responseFromAcceptor = PREPARE(paxosIdentifier);
        String[] responses = responseFromAcceptor.split(":");
        String responseType = responses[0];
        String responseValue = responses[1];
        if(responseType.equals("PROMISE")) {
            trackerObject.get(paxosIdentifier).countPromises++;
        }

        // Call to the rest of them
        String highest_accepted_id = "";
        String highest_accepted_value = "";


        for(FileSystemPaxos.Client paxosServer : paxosServers) {

            try {

                responseFromAcceptor = failSafePREPARE(paxosServer, paxosIdentifier).get(1, TimeUnit.SECONDS);
                responses = responseFromAcceptor.split(":");
                responseType = responses[0];
                responseValue = responses[1];

                logger.logInfo("RESPONSE FOR THE PROMISE REQUEST TO THE SERVER " + responseType);
                if(responseType.equals("PROMISE")) {
                    // A way of tell that you received accepted_id and accepted_value along with your promise
                    if(responses.length == 4) {
                        // The acceptor previously accepted a value
                        logger.logInfo("AN ACCEPTED VALUE IS FOUND YOUR REQUEST WILL BE IGNORED");
                        trackerObject.get(maxRoundIdentifier).countPromises++;
                        String received_accepted_id = responses[2];
                        String received_accepted_value = responses[3];
                        if(Float.parseFloat(received_accepted_id) > Float.parseFloat(highest_accepted_id)) {
                            highest_accepted_value = received_accepted_value;
                            message = updateMessage(message, highest_accepted_value);
                        }
                        trackerObject.get(maxRoundIdentifier).countPromises++;
                    }
                    else {
                        trackerObject.get(maxRoundIdentifier).countPromises++;
                    }
                }

            }

            catch (InterruptedException e) {
                logger.logInfo("ACCEPTOR SEEMS OFFLINE DIDNT RETURN PROMISE");
            }
            catch (ExecutionException e) {
                logger.logInfo("ACCEPTOR SEEMS OFFLINE DIDNT RETURN PROMISE");
            }
            catch (TimeoutException e) {
                logger.logInfo("ACCEPTOR TIMED OUT");
            }

        }

        logger.logInfo("NUMBER OF PROMISES ACHIEVED ON THE PROPOSER " + trackerObject.get(paxosIdentifier).countPromises);
        logger.logInfo("NUMBER OF PROMISES REQUIRED FOR MAJORITY " + majority);
        logger.logInfo("MESSAGE TO BE SENT TO ACCEPTOR " + message);

        int promises = trackerObject.get(paxosIdentifier).countPromises;


        // If promises form a  majority
        if(promises >= majority) {
            logger.logInfo("CALL TO THE LOCAL ACCEPTOR");
            String val2 = this.ACCEPT(paxosIdentifier, message);
            logger.logInfo("RESPONSE FROM THE LOCAL ACCEPTOR " + val2);

            for(FileSystemPaxos.Client paxosServer : paxosServers) {
                try {

                    String val = failSafeACCEPT(paxosServer, paxosIdentifier, message).get(1, TimeUnit.SECONDS);
                    logger.logInfo("RESPONSE FROM THE OTHER ACCEPTOR " + val);
                }

                catch (InterruptedException e) {
                    logger.logInfo("ACCEPTOR SEEMS OFFLINE DIDNT RETURN PROMISE");
                } catch (ExecutionException e) {
                    logger.logInfo("ACCEPTOR SEEMS OFFLINE DIDNT RETURN PROMISE");
                } catch (TimeoutException e) {
                    logger.logInfo("ACCEPTOR TIMED OUT");
                }

            }
        }
        else {
            logger.logInfo("PROMISE FOR THIS PID IS NOT POSSIBLE TRYING FOR THE HIGHER ONE " + maxRoundIdentifier);
            proposerHelper(message);
        }
    }

    private String updateMessage(String message, String value) {
        String[] arr = message.split(":");
        return arr[0] + value;
    }



    private Future<String> failSafePREPARE(final FileSystemPaxos.Client paxosServer, final double paxosIdentifier) {
        ExecutorService executor = Executors.newCachedThreadPool();
        Callable<String> task = new Callable<String>() {
            public String call() {
                try {
                    return paxosServer.PREPARE(paxosIdentifier);
                }
                catch (TException e) {
                    logger.logInfo("SERVER OFFLINE");
                }
                return "OFFLINE:" + paxosIdentifier;
            }
        };
        Future<String> future = executor.submit(task);
        return future;
    }

    private Future<String> failSafeACCEPT(final FileSystemPaxos.Client paxosServer,
                                          final double paxosIdentifier,
                                          final String message) {
        ExecutorService executor = Executors.newCachedThreadPool();
        Callable<String> task = new Callable<String>() {
            public String call() {
                try {
                    return paxosServer.ACCEPT(paxosIdentifier,message);
                }
                catch (TException e) {
                    logger.logInfo("SERVER OFFLINE");
                }
                return "OFFLINE:" + paxosIdentifier + message;
            }
        };
        Future<String> future = executor.submit(task);
        return future;
    }

    private Future<String> failSafeLEARN(final FileSystemPaxos.Client paxosServer,
                                         final double paxosIdentifier,
                                         final String message) {
        ExecutorService executor = Executors.newCachedThreadPool();
        Callable<String> task = new Callable<String>() {
            public String call() {
                try {
                    return paxosServer.LEARN(paxosIdentifier,message);
                }
                catch (TException e) {
                    logger.logInfo("SERVER OFFLINE");
                }
                return "OFFLINE";
            }
        };
        Future<String> future = executor.submit(task);
        return future;
    }



    @Override
    public String PREPARE(double paxosIdentifier){

            logger.logInfo("INSIDER PREPARE");
            logger.logInfo("PAXOSID RECEIVED " + paxosIdentifier);
            // Data structure declaration step as the acceptor may not have initialize the values
            if(!trackerObject.containsKey(paxosIdentifier)) {
                trackerObject.put(paxosIdentifier,new variableCollection());
                logger.logInfo("TRACKER OBJECT CREATED " + trackerObject.toString());
            }

            if(paxosIdentifier <= this.maxRoundIdentifier) {
                return "IGNORED:" + paxosIdentifier;
            }
            else {
                this.maxRoundIdentifier = paxosIdentifier;
                if(trackerObject.containsKey(paxosIdentifier)) {
                    if(trackerObject.get(paxosIdentifier).proposal_accepted) {
                        logger.logInfo("PROPOSAL ALREADY ACCEPTED FOR PAXOS ID");
                        double accepted_ID = trackerObject.get(paxosIdentifier).accepted_ID;
                        String accepted_VALUE = trackerObject.get(paxosIdentifier).accepted_VALUE;
                        return "PROMISE:"  + paxosIdentifier + ":" + accepted_ID + ":" + accepted_VALUE ;
                    }
                    else {
                        return "PROMISE:"  + paxosIdentifier;
                    }

                }
                else {
                    return "PROMISE:"  + paxosIdentifier;
                }
            }


    }

    @Override
    public String ACCEPT(double paxosIdentifier, String value){

            logger.logInfo("INSIDER ACCEPT");
            logger.logInfo("PAXOS ID RECEIVED IN ACCEPT " + paxosIdentifier);
            logger.logInfo("VALUE TO BE ACCEPTED " + value);

            if(paxosIdentifier == this.maxRoundIdentifier) {
                this.trackerObject.get(paxosIdentifier).proposal_accepted = true;
                this.trackerObject.get(paxosIdentifier).accepted_ID = paxosIdentifier;
                this.trackerObject.get(paxosIdentifier).accepted_VALUE = value;

                logger.logInfo("INITIATING LEARN ON THE LOCAL LEARNER FOR " + paxosIdentifier + " " + value);
                LEARN(paxosIdentifier,value);
                int i = 0;
                for(FileSystemPaxos.Client paxosServer : paxosServers) {
                    i++;
                    try {

                        // paxosServer.LEARN(paxosIdentifier,value);
                        failSafeLEARN(paxosServer, paxosIdentifier, value).get(1, TimeUnit.SECONDS);
                        logger.logInfo("SENT ACCEPTANCE TO A LEARNER " + i + " " + paxosIdentifier + " " + value);
                    }
                    // Uncomment to use normally
                    // catch (TException e) {
                    // logger.logInfo("UNABLE TO CONTACT THE LEARNER " + i);
                    //}
                    catch (InterruptedException e) {
                        logger.logInfo("ACCEPTOR SEEMS OFFLINE DIDNT RETURN PROMISE");

                    }
                    catch (ExecutionException e) {
                        logger.logInfo("ACCEPTOR SEEMS OFFLINE DIDNT RETURN PROMISE");

                    }
                    catch (TimeoutException e) {
                        logger.logInfo("ACCEPTOR TIMED OUT");
                    }
                }

                return "ACCEPTED " + paxosIdentifier;
            }

            else {
                return "IGNORED " + paxosIdentifier;
            }

    }

    @Override
    public String LEARN(double paxosIdentifier, String value){


            if(trackerObject.containsKey(paxosIdentifier)) {
                logger.logInfo("INSIDE LEARN");
                logger.logInfo("RECEIVED PAXOS ID " + paxosIdentifier);

                logger.logInfo("VALUE TO BE LEARNED " + value);
                trackerObject.get(paxosIdentifier).countAccepts++;
                int val = trackerObject.get(paxosIdentifier).countAccepts;
                logger.logInfo("UPDATED COUNT OF ACCEPTS " + val);
                if (this.trackerObject.get(paxosIdentifier).countAccepts == majority) {
                    logger.logInfo("CALL TO THE RELEVANT OPERATION WHEN MAJORITY IS ACHIVED " + val);
                    trackerObject.get(paxosIdentifier).proposal_accepted = false;
                    this.operation(value);

                    logger.logInfo("LEARN OPERATION COMPLETED " + val);
                    return "LEARNED";
                } else if (this.trackerObject.get(paxosIdentifier).countAccepts > majority) {
                    return "IGNORED AS THE MAJORITY EXCEEDED";
                } else {
                    return "IGNORED";
                }
            }
        return "LEARN";
    }

    private String operation(String value) {
//        String[] input = value.split(":");
//        String operation = input[0];
//
//        logger.logInfo("PERFORM OPERATION " + value);
//        if(operation.equals("PUT")) {
//            logger.logInfo("PERFORM PUT");
//            String keynode = input[1];
//            String valuenode = input[2];
//            logger.logInfo("PERFORM PUT FOR KEY " + keynode);
//            logger.logInfo("PERFORM PUT FOR VALUE " + valuenode);
//            this.doPUT(keynode, valuenode);
//        }
//        else if(operation.equals("DELETE")) {
//            logger.logInfo("PERFORM DELETE");
//            String keynode = input[1];
//            logger.logInfo("PERFORM PUT FOR DELETE " + keynode);
//            this.doDelete(keynode);
//        }
//        else {
//            logger.logInfo("INVALID OPERATION REQUESTED");
//        }

        return "DONE";
    }

    private String doPUT(String key, String value) {
//        this.store.put(key, value);
//        logger.logInfo("UPDATED STORE VALUE " + this.store.toString());
        return "DONE";
    }

    private String doDelete(String key) {
//        this.store.remove(key);
//        logger.logInfo("UPDATED STORE VALUE " + this.store.toString());
        return "DONE";
    }



}
