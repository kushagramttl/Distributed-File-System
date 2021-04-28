package server;

import chunk.Chunk;
import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import Helper.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 *
 */
public class FileSystemImpl implements FileSystem.Iface {

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
     * fileLocator keeps track of file names and the port number of which chunk they reside in
     * loadTracker keeps track of chunk port numbers and the amount of files the chunks contain.
     * replicaTracker keeps track of each chunk's active replicas.
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
        this.logger = logger;
        this.fileLocator = new ConcurrentHashMap<>();
        this.loadTracker = new ConcurrentHashMap<>();
        this.replicaTracker = new ConcurrentHashMap<>();
        retrieveMetadata();
        for (int port : loadTracker.keySet()){
            replicaTracker.put(port, new HashSet<>());
        }
    }


    private void retrieveMetadata() {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            try (ClientSession session = mongoClient.startSession()) {
                try {
                    MongoDatabase db = mongoClient.getDatabase("FileSystem");
                    MongoCollection<Document> collection = db.getCollection("Metadata");

                    logger.logInfo("Starting transaction to retrieve metadata from DB");

                    session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).build());
                    FindIterable<Document> docs = collection.find();
                    session.commitTransaction();

                    logger.logInfo("Completed transaction to fetch metadata");

                    for (Document doc : docs) {
                        fileLocator.put((String) doc.get("filename"), (Integer) doc.get("chunk"));
                        loadTracker.put((Integer) doc.get("chunk"), (Integer) doc.get("chunk size"));

                    }
                } catch (MongoCommandException e) {
                    logger.logErr("Issue occurred while fetching metadata. Aborting transaction...");
                    session.abortTransaction();
                }
            }
        }
    }

    /**
     * Backs up both the data of the fileLocator and loadTracker maps in the database
     */
    private void backupMetadata() {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            try (ClientSession session = mongoClient.startSession()) {
                try {
                    MongoDatabase db = mongoClient.getDatabase("FileSystem");
                    db.getCollection("Metadata").drop();
                    db.createCollection("Metadata");
                    MongoCollection<Document> collection = db.getCollection("Metadata");

                    logger.logInfo("Starting transaction to update metadata..");

                    session.startTransaction(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());
                    for (Map.Entry entry : fileLocator.entrySet()) {
                        Document data = new Document("_id", new ObjectId());
                        data.append("filename", entry.getKey())
                                .append("chunk", entry.getValue())
                                .append("chunk size", loadTracker.get(entry.getValue()));
                        collection.insertOne(data);
                    }
                    session.commitTransaction();
                    logger.logInfo("Transaction completed");
                } catch (MongoCommandException e) {
                    logger.logErr("Issue occurred while updating metadata. Aborting transaction...");
                    session.abortTransaction();
                }
            }
        }
    }

    @Override
    public ByteBuffer getFile(String name) throws TException {
        if (!fileLocator.containsKey(name)) {
            throw new TApplicationException("File not found");
        }

        retrieveMetadata();

        int chunkPort = fileLocator.get(name);
        chunkPort = getReplicaChunkPort(chunkPort);

        logger.logInfo("Contacting the chunk server: " + chunkPort);

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
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void uploadFile(String name, ByteBuffer file) throws TException {
        retrieveMetadata();
        Map.Entry<Integer, Integer> chunkServer = Collections.min(loadTracker.entrySet(), new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> entry1, Map.Entry<Integer, Integer> entry2) {
                return entry1.getValue().compareTo(entry2.getValue());
            }
        });

        int chunkPort = getReplicaChunkPort(chunkServer.getKey());

        logger.logInfo("Contacting the chunk server: " + chunkPort);

        try (TTransport transport = new TSocket("localhost", chunkPort)) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);
            chunkClient.uploadFile(name, file);

            logger.logInfo("REQUEST - UPLOAD; CHUNKSERV => " + chunkPort + " - FILE => " + name + " - SIZE => " + file.array().length * 1000 + " KB");
            loadTracker.replace(chunkServer.getKey(), chunkServer.getValue() + 1);
            fileLocator.put(name, chunkServer.getKey());
            backupMetadata();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
        } catch (TApplicationException exception) {
            logger.logErr("Error occurred while uploading file. Exception found: " + exception.getMessage());
            throw new TApplicationException("Error while uploading the file");
        }
    }

    @Override
    public void updateFile(String name, ByteBuffer file) throws TException {
        retrieveMetadata();
        if (!fileLocator.containsKey(name)) {
            throw new TApplicationException("File not found");
        }
        int chunkPort = fileLocator.get(name);
        chunkPort = getReplicaChunkPort(chunkPort);

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
        } catch (TApplicationException exception) {
            logger.logErr("Error occurred while updating file. Exception found: " + exception.getMessage());
            throw new TApplicationException("Error while updating the file");
        }
    }

    @Override
    public void deleteFile(String name) throws TException {
        retrieveMetadata();
        if (!fileLocator.containsKey(name)) {
            throw new TApplicationException("File not found");
        }
        int chunkPort = getReplicaChunkPort(fileLocator.get(name));

        logger.logInfo("Contacting the chunk server: " + chunkPort);

        try (TTransport transport = new TSocket("localhost", chunkPort)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            chunkClient.deleteFile(name);
            logger.logInfo("REQUEST - DELETE; CHUNKSERV => " + chunkPort + " - FILE => " + name);

            loadTracker.replace(fileLocator.get(name), loadTracker.get(fileLocator.get(name)) - 1);
            fileLocator.remove(name);
            backupMetadata();


            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
        } catch (TApplicationException exception) {
            logger.logErr("Error occurred while deleting file. Exception found: " + exception.getMessage());
            throw new TApplicationException("Error while deleting the file");
        }
    }

    @Override
    public boolean registerChunk(int port, int replicaPort) throws TException {
        boolean toReturn = false;
        if (replicaPort == -1) { // Not a replica
            if (!loadTracker.containsKey(port) && !replicaTracker.containsKey(port)) {
                loadTracker.put(port, 0);
                replicaTracker.put(port, new HashSet<>());
                toReturn = true;
                logger.logInfo("REQUEST - REGISTER CHUNK; PORT => " + port);
            } else
                logger.logInfo("WARNING - tried to register already registered chunk port");
        } else { // is a replica
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

    /**
     * Method to get the replica port for the main chunk.
     *
     * @param chunkPort The port number for the main chunk port.
     * @return The replica port.
     */
    private int getReplicaChunkPort(int chunkPort) throws TApplicationException {

        try (TTransport transport = new TSocket("localhost", chunkPort)) {
            transport.open();
        } catch (TTransportException e) {
            logger.logInfo("WARNING - CHUNKSERV => " + chunkPort + " is not responding, contacting replica...");
            if (replicaTracker.get(chunkPort).size() == 0)
                throw new TApplicationException("No copy of the requested data has been found");

            for (int replicaPort :
                    replicaTracker.get(chunkPort)) {

                try (TTransport transport = new TSocket("localhost", replicaPort)) {
                    transport.open();
                    chunkPort = replicaPort;
                    break;
                } catch (TTransportException ex) {
                    logger.logErr("Replica Chunk did not respond. Checking for the next one in queue..");
                }
            }

        }


        return chunkPort;
    }
}
