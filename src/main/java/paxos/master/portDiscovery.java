package paxos.master;

import Helper.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class portDiscovery {
    private static int port;
    private static ArrayList<Integer> serverPorts;
    private static PortDiscoveryServiceImpl portDiscoveryImpl;
    private static PortDiscoveryService.Processor<PortDiscoveryServiceImpl> processor;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.print("usage <port-number> <server-ports>\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        try {
            port = Integer.parseInt(args[0]);
            String[] ports = args[1].replaceAll("\\s", "").split(",");
            List<String> inputList = Arrays.asList(ports);
            serverPorts = new ArrayList<>();
            inputList.forEach(port -> serverPorts.add(Integer.parseInt(port)));
        } catch (NumberFormatException e) {
            System.out.print("Please provide an Integer as port number\n");
            Runtime.getRuntime().halt(0); // to not trigger shutdown hook
        }
        Logger logger = new Logger("Port Discovery Service (port:" + port + ")");

        try (TServerTransport serverTransport = new TServerSocket(port)) {

            portDiscoveryImpl = new PortDiscoveryServiceImpl(logger, serverPorts);

            processor = new PortDiscoveryService.Processor<PortDiscoveryServiceImpl>(portDiscoveryImpl);

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            logger.logInfo("Starting the port discovery service at port " + port);
            server.serve();
            // SHUTDOWN procedure
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
                serverTransport.close();
                System.out.println("Service is shutting down, closing all sockets...");
            }));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
