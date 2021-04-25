package paxos;

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
import java.util.concurrent.ConcurrentHashMap;



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
public class FileSystemImpl implements FileSystem.Iface, FunctionalityOfPaxosStore.Iface {


    /**
     * Variables related to paxos prepare, accept and learn
     */

    private double maxRoundIdentifier;
    private int majority;
    private Map<Double, variableCollection> trackerObject;
    private List<FunctionalityOfPaxosStore.Client> paxosServers;
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
    public FileSystemImpl(Logger logger) {


        //
        this.maxRoundIdentifier = 1 + Math.random();
        // servers.size()/2 + 1; should be this if prvided with the connecting servers list
        this.majority = 5;
        this.trackerObject = new HashMap<Double, variableCollection>();
        this.paxosServers = getServers(servers);



        this.logger = logger;
        this.fileLocator = new ConcurrentHashMap<>();
        this.loadTracker = new ConcurrentHashMap<>();
        this.replicaTracker = new ConcurrentHashMap<>();
        retrieveMetadata();
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
    private List<FunctionalityOfPaxosStore.Client> getServers(List<ServerIdentifier> paxosServerIdentifiers) {

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<FunctionalityOfPaxosStore.Client> clients = new ArrayList<FunctionalityOfPaxosStore.Client>();

        log.logInfoMessage("Inside the functionality implementation paxosServers.size()" + paxosServerIdentifiers.size() );

        for(int i = 0; i < paxosServerIdentifiers.size(); i++) {
            if(paxosServerIdentifiers.get(i).getPort() != port) {
                FunctionalityOfPaxosStore.Client paxosClient = null;
                ServerIdentifier participant = paxosServerIdentifiers.get(i);


                try {

                    String host = participant.getHostname();
                    int port = participant.getPort();
                    TSocket transport = new TSocket(host, port);
                    transport.open();
                    TBinaryProtocol protocol = new TBinaryProtocol(transport);
                    paxosClient = new FunctionalityOfPaxosStore.Client(protocol);
                    clients.add(paxosClient);
                }
                catch (TTransportException e) {
                    e.printStackTrace();
                }

            }


            log.logInfoMessage("clients.size() " + clients.size());
            log.logInfoMessage("clients.toString() " + clients.toString());



        }

        return clients;
    }



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


        Map.Entry<Integer, Integer> chunkServer = Collections.min(loadTracker.entrySet(), new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> entry1, Map.Entry<Integer, Integer> entry2) {
                return entry1.getValue().compareTo(entry2.getValue());
            }
        });

        try (TTransport transport = new TSocket("localhost", chunkServer.getKey())) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);
            chunkClient.uploadFile(name, file);

            logger.logInfo("REQUEST - UPLOAD; CHUNKSERV => " + chunkServer.getKey() + " - FILE => " + name + " - SIZE => " + file.array().length * 1000 + " KB");
            loadTracker.replace(chunkServer.getKey(), chunkServer.getValue() + 1);
            fileLocator.put(name, chunkServer.getKey());
            backupMetadata();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
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
        int chunkPort = fileLocator.get(name);

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


    // Methods need for paxos

    public void dummyFunction() {

    }




    @Override
    public String PREPARE(double paxosIdentifier){

        log.logInfoMessage("INSIDER PREPARE");
        // Data structure declaration step as the acceptor may not have initialize the values
        if(!trackerObject.containsKey(paxosIdentifier)) {
            trackerObject.put(paxosIdentifier,new variableCollection());
        }

        if(paxosIdentifier <= this.maxRoundIdentifier) {
            return "IGNORED:" + paxosIdentifier;
        }
        else {
            this.maxRoundIdentifier = paxosIdentifier;

            if(trackerObject.containsKey(paxosIdentifier)) {
                if(trackerObject.get(paxosIdentifier).proposal_accepted) {
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


        log.logInfoMessage("INSIDER ACCEPT");
        log.logInfoMessage("VALUE TO BE ACCEPTED " + value);

        if(paxosIdentifier == this.maxRoundIdentifier) {
            this.trackerObject.get(paxosIdentifier).proposal_accepted = true;
            this.trackerObject.get(paxosIdentifier).accepted_ID = paxosIdentifier;
            this.trackerObject.get(paxosIdentifier).accepted_VALUE = value;

            LEARN(paxosIdentifier,value);
            for(FunctionalityOfPaxosStore.Client paxosServer : paxosServers) {
                try {

                    paxosServer.LEARN(paxosIdentifier,value);

                }
                catch (TException e) {
                    e.printStackTrace();
                }
            }


            return "ACCEPTED " + paxosIdentifier;

        }


        else {
            return "IGNORED";
        }


    }

    @Override
    public String LEARN(double paxosIdentifier, String value){
        // Updates the acceptance count
        // this.acceptsReceived++;


        log.logInfoMessage("INSIDER LEARN");
        log.logInfoMessage("VALUE TO BE LEARNED " + value);

        trackerObject.get(paxosIdentifier).countAccepts++;

//        log.logInfoMessage("trackerObject.get(paxosIdentifier).countAccepts++; " + trackerObject.get(paxosIdentifier).countAccepts++);

        // And then calls the check value which will fail for the rest of them
        if(this.trackerObject.get(paxosIdentifier).countAccepts == majority) {
            // Need to get the value in format GET:KEY or PUT:KEY:VALUE or DELETE:KEY:VALUE
            // Need to set the pid now so that history cannot be changed
            this.operation(value);
            return "LEARNED";
        }
        else if(this.trackerObject.get(paxosIdentifier).countAccepts > majority) {
            return "IGNORED";
        }
        else {
            // Ignore for the rest
            return "IGNORED";
        }

    }



    private String operation(String value) {
        return "";
    }


    // Unneccessary methods wil  be removed soon
    @Override
    public String GET(String key) throws TException {

        return null;
    }

    @Override
    public String PUT(String key, String value) throws TException {
        return null;
    }

    @Override
    public String DELETE(String key) throws TException {
        return null;
    }


}
