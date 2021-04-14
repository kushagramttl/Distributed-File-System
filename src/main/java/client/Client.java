package client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import server.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import Helper.Logger;

public class Client {

  private int port;
  private Logger logger;
  private FileSystem.Client fileSystemClient;
  private String hostName;


  public Client(int port, Logger logger, String hostName) {
    this.port = port;
    this.logger = logger;
    this.hostName = hostName;
  }

  public void sendRequest() {
    try (TTransport transport = new TSocket(hostName, port)){
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      fileSystemClient = new FileSystem.Client(protocol);

      fileSystemClient.uploadFile("Trial.txt", null);

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        transport.close();

        System.out.println("Client is shutting down, closing all sockets!");
      }));

    } catch (TException e) {
      logger.logErr("Failure on the server side: " + e.getMessage());
      System.exit(0); // triggers shutdown hook
    }
  }

}
