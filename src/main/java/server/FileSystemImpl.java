package server;


import org.apache.thrift.TException;

import java.nio.ByteBuffer;

import Helper.Logger;

/**
 *
 */
public class FileSystemImpl implements FileSystem.Iface {

  /**
   * The logger object.
   */
  private Logger logger;

  /**
   * Creates a new instance of the FileSystemImpl
   *
   * @param logger The logger for logging.
   */
  public FileSystemImpl(Logger logger) {
    this.logger = logger;
  }

  @Override
  public ByteBuffer getFile(String name) throws TException {
    return null;
  }

  @Override
  public void uploadFile(String name, ByteBuffer file) throws TException {
    this.logger.logInfo("Inside upload file");
    this.logger.logInfo("Name received: " + name);
  }

  @Override
  public void updateFile(String name, ByteBuffer file) throws TException {

  }

  @Override
  public void deleteFile(String name) throws TException {

  }
}
