package paxos.master;

import Helper.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.ArrayList;
import java.util.List;

public class Server {

    private static int port;
    private static ArrayList<Integer> chunkPorts;
    private static FileSystemPaxosImpl fileSystem;
    private static FileSystemPaxos.Processor<FileSystemPaxosImpl> processor;

    private static List<ServerIdentifier> getServerIdentifer() {
        List<ServerIdentifier> servers = new ArrayList<ServerIdentifier>();
        servers.add(new ServerIdentifier("localhost", 1000));
        servers.add(new ServerIdentifier("localhost", 1100));
        servers.add(new ServerIdentifier("localhost", 1200));
        servers.add(new ServerIdentifier("localhost", 1300));
        return servers;
    }

    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.print("usage <port-number>\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        try {
            port = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.out.print("Please provide an Integer as port number\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        Logger logger = new Logger("File System Server (port:" + port + ")");

        try (TServerTransport serverTransport = new TServerSocket(port)) {
            List<ServerIdentifier> servers = getServerIdentifer();
            fileSystem = new FileSystemPaxosImpl(logger, servers, port);
            processor = new FileSystemPaxos.Processor<FileSystemPaxosImpl>(fileSystem);

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            logger.logInfo("Starting the Distributed FileSystem service at port: " + port);
            server.serve();
            // SHUTDOWN procedure
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
                serverTransport.close();
                System.out.println("Server is shutting down, closing all sockets!");
            }));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
