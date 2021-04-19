package server;

import chunk.Chunk;
import com.mongodb.client.*;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import Helper.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
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
     *  fileLocator keeps track of file names and the port number of which chunk they reside in
     *  loadTracker keeps track of chunk port numbers and the amount of files the chunks contain.
     */
    private Map<String, Integer> fileLocator;
    private Map<Integer, Integer> loadTracker;

    /**
     * Creates a new instance of the FileSystemImpl
     *
     * @param logger The logger for logging.
     */
    public FileSystemImpl(Logger logger, ArrayList<Integer> chunkPorts) {
        this.logger = logger;
        this.fileLocator = new ConcurrentHashMap<>();
        this.loadTracker = new ConcurrentHashMap<>();
        for (int port : chunkPorts){
            this.loadTracker.put(port, 0);
        }
        retrieveMetadata();
    }

    // TODO IMPORTANT: improve these two methods to not delete and recreate metadata but rather just update it in a consistent manner

    private void retrieveMetadata(){
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> collection = db.getCollection("Metadata");
            FindIterable<Document> docs = collection.find();

            for (Document doc : docs) {
                fileLocator.put((String) doc.get("filename"), (Integer) doc.get("chunk"));
                loadTracker.put((Integer) doc.get("chunk"), (Integer) doc.get("chunk size"));
            }
        }
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
}
