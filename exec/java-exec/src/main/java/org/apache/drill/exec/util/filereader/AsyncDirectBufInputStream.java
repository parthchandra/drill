package org.apache.drill.exec.util.filereader;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by pchandra on 6/29/16.
 */
public class AsyncDirectBufInputStream extends DirectBufInputStream {
  AsyncDirectBufInputStream(InputStream in, boolean enableHints) {
    super(in, enableHints);
  }

  @Override public void init() throws IOException, UnsupportedOperationException {

  }

  @Override public int read() throws IOException {
    return 0;
  }

  @Override public int read(DrillBuf buf, int off, int len) throws IOException {
    return 0;
  }

  @Override public DrillBuf getNext(int bytes) throws IOException {
    return null;
  }

  @Override public long getPos() throws IOException {
    return 0;
  }

  @Override public boolean hasRemainder() throws IOException {
    return false;
  }
}
