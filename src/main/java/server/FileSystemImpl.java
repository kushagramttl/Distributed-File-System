package server;

import static java.util.Arrays.asList;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import Helper.Logger;
import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.conversions.Bson;
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
     * Creates a new instance of the FileSystemImpl
     *
     * @param logger The logger for logging.
     */
    public FileSystemImpl(Logger logger) {
        this.logger = logger;
    }

    @Override
    public ByteBuffer getFile(String name) throws TException {
        String connectionString = System.getProperty("mongodb.uri");

        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> gradesCollection = db.getCollection("Files");

            Document file = gradesCollection.find(new Document("filename", name)).first();
            String filename = (String) file.get("filename");
            Binary data = (Binary) file.get("data");

            try (FileOutputStream stream = new FileOutputStream(filename)) {
                stream.write(data.getData());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void uploadFile(String name, ByteBuffer file) throws TException {
        this.logger.logInfo("Name received: " + name);

        String connectionString = System.getProperty("mongodb.uri");

        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase("FileSystem");
            MongoCollection<Document> gradesCollection = db.getCollection("Files");

            Random rand = new Random();
            Document data = new Document("_id", new ObjectId());
            data.append("filename", name)
                    .append("data", new Binary(file.array()));

            gradesCollection.insertOne(data);
        }
    }

    @Override
    public void updateFile(String name, ByteBuffer file) throws TException {

    }

    @Override
    public void deleteFile(String name) throws TException {

    }
}
