package chunk;

import Helper.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import server.FileSystem;
import server.PortDiscoveryService;

public class portDiscoveryClient {
    public static void castPort(int port, int replicaPort, int discoveryPort, Logger logger) throws TException {
        try (TTransport transport = new TSocket("localhost", discoveryPort)) {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            PortDiscoveryService.Client portDiscoveryService = new PortDiscoveryService.Client(protocol);
            portDiscoveryService.registerChunk(port, replicaPort);

            logger.logInfo("Casting port to Port Discovery Service...");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                transport.close();

            }));
        }
    }
}
