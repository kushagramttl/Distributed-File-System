package chunk;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;

/**
 *
 */
public class ChunkImpl implements Chunk.Iface {

  @Override
  public ByteBuffer getFile(String name) throws TException {
    return null;
  }

  @Override
  public boolean containsFile(String name) throws TException {
    return false;
  }

  @Override
  public void deleteFile(String name) throws TException {

  }

  @Override
  public ByteBuffer getMetadata() throws TException {
    return null;
  }

  @Override
  public void uploadFile(String name, ByteBuffer file) throws TException {

  }

  @Override
  public void updateFile(String name, ByteBuffer file) throws TException {

  }
}
