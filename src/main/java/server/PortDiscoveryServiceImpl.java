package server;

import Helper.Logger;
import chunk.Chunk;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Centralized Service that receives registration requests from the chunk servers
 *
 * If it fails, the system cannot register any more chunk servers. The system however remains fully operational
 *
 * edge case: if this fails while registering chunk servers with active collections, parts of the database might become inaccessible.
 */
public class PortDiscoveryServiceImpl implements PortDiscoveryService.Iface{
    /**
     * The logger object.
     */
    private Logger logger;

    /**
     * Ports of the master servers.
     */
    private ArrayList<Integer> serverPorts;

    /**
     *
     * @param logger
     * @param serverPorts
     */
    public PortDiscoveryServiceImpl(Logger logger, ArrayList<Integer> serverPorts) {
        this.logger = logger;
        this.serverPorts = serverPorts;
    }

    @Override
    public boolean registerChunk(int port, int replicaPort) throws TException {
        for (int sever : serverPorts) {
            try (TTransport transport = new TSocket("localhost", sever)) {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                FileSystem.Client server = new FileSystem.Client(protocol);
                server.registerChunk(port, replicaPort);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    transport.close();

                    logger.logInfo("Port Discovery service shutting down, closing connections...");
                }));
            }
        }
        return false;
    }
}
