package client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TApplicationException;

import server.FileSystem;

import Helper.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

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

      ByteBuffer binaryArray;
      // TODO : Make user input absolute path.
      binaryArray = ByteBuffer.wrap(Files.readAllBytes(
              Paths.get("/home/soufianej/Documents/Courses/Building-scalable-distributed-systems/Projects/final-project/DistributedStorage/src/main/java/client/Test.txt")));

      fileSystemClient.uploadFile("Test3.txt", binaryArray);
//      ByteBuffer data = fileSystemClient.getFile("Test.txt");
//
//      try (FileOutputStream stream = new FileOutputStream("Test.txt")) {
//        stream.write(data.array());
//      } catch (IOException e) {
//        e.printStackTrace();
//      }


//        fileSystemClient.deleteFile("Test.txt");

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        transport.close();

        System.out.println("Client is shutting down, closing all sockets!");
      }));

    } catch (TApplicationException exception) {
      logger.logErr("Error received from server: " + exception.getMessage());
    } catch (TException | IOException e) {
      logger.logErr("Failure on the server side: " + e.getMessage());
      e.printStackTrace();
      System.exit(0); // triggers shutdown hook
    }
  }

}
