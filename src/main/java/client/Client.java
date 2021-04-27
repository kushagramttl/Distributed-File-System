package client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.TimeZone;

import Helper.Logger;
import server.FileSystem;

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


  public void acceptRequests() {
    Scanner read = new Scanner(System.in);
    Request request;

    try (TTransport transport = new TSocket(hostName, port)) {
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      fileSystemClient = new FileSystem.Client(protocol);


      System.out.println("-----------------------------------------------------------");
      System.out.println(getCurrentTime() + " Type in your requests in the following format");
      System.out.println("UPLOAD: \"UPLOAD,<file_name>,<file_path>\"");
      System.out.println("GET: \"GET,<file_name>\"");
      System.out.println("DELETE: \"DELETE,<file_name>\"");
      System.out.println("quit or close to exit");
      System.out.println("-----------------------------------------------------------\n");

      for (; ; ) {
        // Accept string from user
        System.out.println("\n" + getCurrentTime() + " Enter new Request");
        String inputString = read.nextLine();

        /*
         * If the request string is explicitly "close" or "quit", close the connection.
         */
        if (inputString.equalsIgnoreCase("close")
                || inputString.equalsIgnoreCase("quit")) {
          break;
        }

        request = Request.fromString(inputString);

        if (request == null) {
          System.out.println(getCurrentTime() + " Wrong format.");
        } else {

          try {
            handleRequest(request);
          } catch (TException exception) {
            logger.logErr("Error received from server: " + exception.getMessage());
          }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          transport.close();
          System.out.println(getCurrentTime() + " Client is shutting down, closing all sockets!");
        }));


      }
    } catch (TTransportException e) {
      logger.logErr("Failure on the server side: " + e.getMessage());
      e.printStackTrace();
      System.exit(0); // triggers shutdown hook
    }
  }


  private void handleRequest(Request request) throws TException {
    ByteBuffer binaryArray;
    switch (request.operation.toString()) {
      case "UPLOAD": {
        System.out.println(getCurrentTime() + " Received UPLOAD " + request.fileName + " from " + request.filePath);
        try {
          binaryArray = ByteBuffer.wrap(Files.readAllBytes(
                  Paths.get(request.filePath)));
          fileSystemClient.uploadFile(request.fileName, binaryArray);
        } catch (Exception e) {
          System.out.println(getCurrentTime() + " Upload failed");
        }
        break;
      }
      case "UPDATE": {
        System.out.println(getCurrentTime() + " Received UPDATE " + request.fileName + " from " + request.filePath);
        try {
          binaryArray = ByteBuffer.wrap(Files.readAllBytes(
                  Paths.get(request.filePath)));
          fileSystemClient.updateFile(request.fileName, binaryArray);
        } catch (Exception e) {
          System.out.println(getCurrentTime() + " Update failed");
        }
        break;
      }
      case "GET": {
        System.out.println(getCurrentTime() + " Received GET " + request.fileName);
        ByteBuffer data = fileSystemClient.getFile(request.fileName);
        if (data == null) {
          System.out.println(getCurrentTime() + "GET failed");
          break;
        }
        try (FileOutputStream stream = new FileOutputStream(request.fileName)) {
          stream.write(data.array());
        } catch (IOException e) {
          e.printStackTrace();
        }
        break;
      }
      case "DELETE": {
        System.out.println("Received DELETE " + request.fileName);
        try {
          fileSystemClient.deleteFile(request.fileName);
        } catch (Exception e) {
          System.out.println(getCurrentTime() + " DELETE failed");
        }
      }
    }
  }

  private String getCurrentTime() {
    Date date = new Date(System.currentTimeMillis());
    DateFormat formatter = new SimpleDateFormat("MM:d:y HH:mm:ss.SSS");
    formatter.setTimeZone(TimeZone.getTimeZone("EST"));
    return formatter.format(date);
  }

}
