package server;

import static java.util.Arrays.asList;

import chunk.Chunk;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Filter;

import Helper.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import javax.print.Doc;

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
     * Creates a new instance of the FileSystemImpl
     *
     * @param logger The logger for logging.
     */
    public FileSystemImpl(Logger logger) {
        this.logger = logger;
    }

    @Override
    public ByteBuffer getFile(String name) throws TException {
        try (TTransport transport = new TSocket("localhost", 8000)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            ByteBuffer buffer = chunkClient.getFile(name);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));


            return buffer;

        }
    }

    @Override
    public void uploadFile(String name, ByteBuffer file) throws TException {
        this.logger.logInfo("Name received: " + name);

        try (TTransport transport = new TSocket("localhost", 8000)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            chunkClient.uploadFile(name, file);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
        }
    }

    @Override
    public void updateFile(String name, ByteBuffer file) throws TException {
        this.logger.logInfo("Name received in update operation: " + name);

        try (TTransport transport = new TSocket("localhost", 8000)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            chunkClient.updateFile(name, file);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
        }
    }

    @Override
    public void deleteFile(String name) throws TException {
        this.logger.logInfo("Name received in delete operation: " + name);

        try (TTransport transport = new TSocket("localhost", 8000)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            chunkClient = new Chunk.Client(protocol);

            chunkClient.deleteFile(name);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

                System.out.println("Client is shutting down, closing all sockets!");
            }));
        }
    }
}
