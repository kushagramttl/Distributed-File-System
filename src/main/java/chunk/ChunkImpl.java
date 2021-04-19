package chunk;

import Helper.Logger;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.io.FileOutputStream;
import java.io.IOException;
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
            MongoDatabase db = mongoClient.getDatabase("FileSystem");

            MongoCollection<Document> collection = db.getCollection(this.collectionName);

            Document file = collection.find(Filters.eq("filename", name)).first();

            if (file == null)
                throw new TApplicationException("File not found");

            String filename = (String) file.get("filename");
            Binary data = (Binary) file.get("data");

            return ByteBuffer.wrap(data.getData());
        }

    }

    @Override
    public void deleteFile(String name) throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> gradesCollection = db.getCollection(this.collectionName);

            DeleteResult result = gradesCollection.deleteOne(Filters.eq("filename", name));

            System.out.println(result);
        }
    }

    @Override
    public ByteBuffer getMetadata() throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> collection = db.getCollection("Files");

            FindIterable<Document> docs = collection.find();

            for (Document d :
                    docs) {
                System.out.println((String) d.get("filename"));
            }
        }

        return null;
    }

    @Override
    public void uploadFile(String name, ByteBuffer file) throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> collection = db.getCollection(this.collectionName);

            Document data = new Document("_id", new ObjectId());
            data.append("filename", name)
                    .append("data", new Binary(file.array()));

            collection.insertOne(data);
        }
    }

    @Override
    public void updateFile(String name, ByteBuffer file) throws TException {
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> collection = db.getCollection(this.collectionName);

            collection.updateOne(Filters.eq("filename", name), Updates.set("data", new Binary(file.array())));
        }
    }
}
