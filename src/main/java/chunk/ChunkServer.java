package chunk;

import Helper.Logger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import server.FileSystem;
import server.FileSystemImpl;

public class ChunkServer {

    private static int port;
    private static int coordinator_port;
    private static ChunkImpl chunk;
    private static Chunk.Processor<ChunkImpl> processor;

    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.print("usage <port-number>\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.print("Please provide an Integer as port number\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        Logger logger = new Logger("Chunk Server (port:" + port + ")");
        try (TServerTransport serverTransport = new TServerSocket(port)) {

            chunk = new ChunkImpl(logger, port);

            processor = new Chunk.Processor<ChunkImpl>(chunk);

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            logger.logInfo("Starting the Key Value Store Service at port: " + port);
            server.serve();
            // SHUTDOWN procedure
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
                serverTransport.close();
                System.out.println("Server is shutting down, closing all sockets!");
            }));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
