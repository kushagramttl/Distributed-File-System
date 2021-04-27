package chunk;

import Helper.Logger;
import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import java.nio.ByteBuffer;

/**
 * A class that holds the implementation for Chunk implementations
 */
public class ChunkImpl implements Chunk.Iface {

    /**
     * The logger object.
     */
    private Logger logger;

    /**
     * Maintains the name of the collection made using the port number.
     */
    private String collectionName;

    /**
     * The connection string to connect to the database.
     */
    private String connectionString;

    /**
     * Creates a new instance of the FileSystemImpl
     *
     * @param logger The logger for logging.
     * @param port   The port number chunk is working on.
     */
    public ChunkImpl(Logger logger, int port) {
        this.logger = logger;
        this.collectionName = "Chunk" + port;
        this.connectionString = System.getProperty("mongodb.uri");
    }

    @Override
    public ByteBuffer getFile(String name) throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            try (ClientSession session = mongoClient.startSession()) {
                try {
                    MongoDatabase db = mongoClient.getDatabase("FileSystem");

                    MongoCollection<Document> collection = db.getCollection(this.collectionName);

                    logger.logInfo("Starting database transaction");

                    session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).build());
                    Document file = collection.find(Filters.eq("filename", name)).first();

                    if (file == null)
                        throw new TApplicationException("File not found");

                    String filename = (String) file.get("filename");

                    if (file.get("data") == null)
                        throw new TApplicationException("File not found");

                    Binary data = (Binary) file.get("data");

                    logger.logInfo("Committing transaction to database");
                    session.commitTransaction();
                    return ByteBuffer.wrap(data.getData());
                } catch (MongoCommandException e) {
                    logger.logErr("Issue occurred while uploading the file. Aborting transaction...");
                    session.abortTransaction();
                }
            }
        }

        return null;
    }

    @Override
    public void deleteFile(String name) throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            try (ClientSession session = mongoClient.startSession()) {
                try {
                    MongoDatabase db = mongoClient.getDatabase("FileSystem");
                    MongoCollection<Document> gradesCollection = db.getCollection(this.collectionName);

                    logger.logInfo("Starting database transaction");

                    session.startTransaction(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());
                    DeleteResult result = gradesCollection.deleteOne(Filters.eq("filename", name));

                    logger.logInfo("Committing transaction to database");
                    session.commitTransaction();
                    System.out.println(result);
                } catch (MongoCommandException e) {
                    logger.logErr("Issue occurred while uploading the file. Aborting transaction...");
                    session.abortTransaction();
                }
            }
        }
    }

    @Override
    public ByteBuffer getMetadata() throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            try (ClientSession session = mongoClient.startSession()) {
                try {
                    MongoDatabase db = mongoClient.getDatabase("FileSystem");
                    MongoCollection<Document> collection = db.getCollection("Files");

                    logger.logInfo("Starting database transaction");

                    session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).build());
                    FindIterable<Document> docs = collection.find();

                    logger.logInfo("Committing transaction to database");
                    session.commitTransaction();

                    for (Document d :
                            docs) {
                        System.out.println((String) d.get("filename"));
                    }
                } catch (MongoCommandException e) {
                    logger.logErr("Issue occurred while uploading the file. Aborting transaction...");
                    session.abortTransaction();
                }
            }
        }

        return null;
    }

    @Override
    public void uploadFile(String name, ByteBuffer file) throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            try (ClientSession session = mongoClient.startSession()) {
                try {
                    MongoDatabase db = mongoClient.getDatabase("FileSystem");
                    MongoCollection<Document> collection = db.getCollection(this.collectionName);

                    logger.logInfo("Starting database transaction");

                    session.startTransaction(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());
                    Document data = new Document("_id", new ObjectId());
                    data.append("filename", name)
                            .append("data", new Binary(file.array()));

                    collection.insertOne(data);

                    logger.logInfo("Committing transaction to database");
                    session.commitTransaction();
                } catch (MongoCommandException e) {
                    logger.logErr("Issue occurred while uploading the file. Aborting transaction...");
                    session.abortTransaction();
                }
            }
        }
    }

    @Override
    public void updateFile(String name, ByteBuffer file) throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            try (ClientSession session = mongoClient.startSession()) {
                try {
                    MongoDatabase db = mongoClient.getDatabase("FileSystem");
                    MongoCollection<Document> collection = db.getCollection(this.collectionName);

                    logger.logInfo("Starting database transaction");

                    session.startTransaction(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());
                    collection.updateOne(Filters.eq("filename", name), Updates.set("data", new Binary(file.array())));

                    logger.logInfo("Committing transaction to database");
                    session.commitTransaction();
                } catch (MongoCommandException e) {
                    logger.logErr("Issue occurred while uploading the file. Aborting transaction...");
                    session.abortTransaction();
                }
            }
        }
    }
}
