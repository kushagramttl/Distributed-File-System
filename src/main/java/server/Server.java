package server;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import Helper.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Server {

    private static int port;
    private static ArrayList<Integer> chunkPorts;
    private static FileSystemImpl fileSystem;
    private static FileSystem.Processor<FileSystemImpl> processor;

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
        Logger logger = new Logger("File System Server (port:" + port + ")");

        try (TServerTransport serverTransport = new TServerSocket(port)) {

            fileSystem = new FileSystemImpl(logger);
            processor = new FileSystem.Processor<FileSystemImpl>(fileSystem);

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            logger.logInfo("Starting the Distributed FileSystem service at port: " + port);
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
