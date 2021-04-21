package chunk;

import Helper.Logger;
import Helper.Utils;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

public class ChunkServer {

    private static int port;
    private static int replicaPort;
    private static int discoveryPort;
    private static ChunkImpl chunk;
    private static Chunk.Processor<ChunkImpl> processor;

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.print("usage <port-number> <replica-port> <discovery-port>\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        if (!Utils.areDistinct(args)){
            System.out.print("Please input distinct port arguments!\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        try {
            port = Integer.parseInt(args[0]);
            replicaPort = Integer.parseInt(args[1]);
            discoveryPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.print("Please provide an Integer as port number\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        Logger logger = new Logger("Chunk Server (port:" + port + ")");
        try (TServerTransport serverTransport = new TServerSocket(port)) {
            // replica port is the port of the replicated server, this port also allows access to the database collection therefore it is passed to the
            // service implementation
            if (replicaPort == -1)
                chunk = new ChunkImpl(logger, port);
            else
                chunk = new ChunkImpl(logger, replicaPort);

            processor = new Chunk.Processor<ChunkImpl>(chunk);

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            portDiscoveryClient.castPort(port, replicaPort, discoveryPort, logger);
            logger.logInfo("Starting the Storage Chunk Service at port: " + port);
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
