/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hama.pipes.protocol;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.pipes.Submitter;

/**
 * This protocol is a binary implementation of the Hama Pipes protocol.
 * 
 * Adapted from Hadoop Pipes
 * 
 * @author Martin Illecker
 * 
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public class BinaryProtocol<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
    implements DownwardProtocol<K1, V1> {

  private static final Log LOG = LogFactory.getLog(BinaryProtocol.class
      .getName());
  public static final int CURRENT_PROTOCOL_VERSION = 0;
  /**
   * The buffer size for the command socket
   */
  public static final int BUFFER_SIZE = 128 * 1024;

  private final DataOutputStream stream;
  private final DataOutputBuffer buffer = new DataOutputBuffer();

  private UplinkReader<K1, V1, K2, V2> uplink;

  private boolean hasTask = false;
  private int result = -1;
  
  /**
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward downward
   * messages are public methods on this object.
   * 
   * @param sock The socket to communicate on.
   * @param handler The handler for the received messages.
   * @param key The object to read keys into.
   * @param value The object to read values into.
   * @param jobConfig The job's configuration
   * @throws IOException
   */
  public BinaryProtocol(BSPPeer<K1, V1, K2, V2, BytesWritable> peer, Socket sock)
      throws IOException {
    OutputStream raw = sock.getOutputStream();

    // If we are debugging, save a copy of the downlink commands to a file
    if (Submitter.getKeepCommandFile(peer.getConfiguration())) {
      raw = new TeeOutputStream("downlink.data", raw);
    }
    stream = new DataOutputStream(new BufferedOutputStream(raw, BUFFER_SIZE));
    uplink = new UplinkReader<K1, V1, K2, V2>(this, peer, sock.getInputStream());

    uplink.setName("pipe-uplink-handler");
    uplink.start();
  }

  /* **************************************************** */
  /* Setter and Getter starts...                          */
  /* **************************************************** */
  
  public boolean isHasTask() {
    return hasTask;
  }

  public synchronized void setHasTask(boolean hasTask) {
    this.hasTask = hasTask;
  }
  
  public synchronized void setResult(int result) {
    this.result = result;
  }

  public DataOutputStream getStream() {
    return stream;
  }
  
  /* **************************************************** */
  /* Setter and Getter ends...                            */
  /* **************************************************** */


  /**
   * An output stream that will save a copy of the data into a file.
   */
  private static class TeeOutputStream extends FilterOutputStream {
    private OutputStream file;

    TeeOutputStream(String filename, OutputStream base) throws IOException {
      super(base);
      file = new FileOutputStream(filename);
    }

    public void write(byte b[], int off, int len) throws IOException {
      file.write(b, off, len);
      out.write(b, off, len);
    }

    public void write(int b) throws IOException {
      file.write(b);
      out.write(b);
    }

    public void flush() throws IOException {
      file.flush();
      out.flush();
    }

    public void close() throws IOException {
      flush();
      file.close();
      out.close();
    }
  }

  /* **************************************************** */
  /* Implementation of DownwardProtocol<K1, V1> begins... */
  /* **************************************************** */

  @Override
  public void start() throws IOException {
    LOG.debug("starting downlink");
    WritableUtils.writeVInt(stream, MessageType.START.code);
    WritableUtils.writeVInt(stream, CURRENT_PROTOCOL_VERSION);
    flush();
    LOG.debug("Sent MessageType.START");
  }

  @Override
  public void setInputTypes(String keyType, String valueType)
      throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_INPUT_TYPES.code);
    Text.writeString(stream, keyType);
    Text.writeString(stream, valueType);
    flush();
    LOG.debug("Sent MessageType.SET_INPUT_TYPES");
  }

  @Override
  public void runSetup(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_SETUP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_SETUP");
  }

  @Override
  public void runBsp(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_BSP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_BSP");
  }

  @Override
  public void runCleanup(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_CLEANUP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_CLEANUP");
  }

  @Override
  public int getPartition(String key, String value, int numTasks)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.PARTITION_REQUEST.code);
    Text.writeString(stream, key);
    Text.writeString(stream, value);
    WritableUtils.writeVInt(stream, numTasks);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.PARTITION_REQUEST");
    
    try {
      waitForFinish(); //wait for response
    } catch (InterruptedException e) {
      LOG.error(e);
    }
    
    return result;
  }

  @Override
  public void abort() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.ABORT.code);
    flush();
    LOG.debug("Sent MessageType.ABORT");
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  /**
   * Close the connection and shutdown the handler thread.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void close() throws IOException, InterruptedException {
    // runCleanup(pipedInput,pipedOutput);
    LOG.debug("closing connection");
    endOfInput();

    uplink.interrupt();
    uplink.join();

    uplink.closeConnection();
    stream.close();
  }
  
  @Override
  public boolean waitForFinish() throws IOException, InterruptedException {
    // LOG.debug("waitForFinish... "+hasTask);
    while (hasTask) {
      try {
        Thread.sleep(100);
        // LOG.debug("waitForFinish... "+hasTask);
      } catch (Exception e) {
        LOG.error(e);
      }
    }
    return hasTask;
  }

  /* **************************************************** */
  /* Implementation of DownwardProtocol<K1, V1> ends... */
  /* **************************************************** */

  
  /* **************************************************** */
  /* Private Class methods begins...                      */
  /* **************************************************** */
  
  private void endOfInput() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.CLOSE.code);
    flush();
    //LOG.debug("Sent close command");
    LOG.debug("Sent MessageType.CLOSE");
  }
  
  /* **************************************************** */
  /* Private Class methods ends...                        */
  /* **************************************************** */
  
  
  /* **************************************************** */
  /* Public Class methods begins...                       */
  /* **************************************************** */
 
  /**
   * Write the given object to the stream. If it is a Text or BytesWritable,
   * write it directly. Otherwise, write it to a buffer and then write the
   * length and data to the stream.
   * 
   * @param obj the object to write
   * @throws IOException
   */
  public void writeObject(Writable obj) throws IOException {
    // For Text and BytesWritable, encode them directly, so that they end up
    // in C++ as the natural translations.
    if (obj instanceof Text) {
      Text t = (Text) obj;
      int len = t.getLength();
      WritableUtils.writeVInt(stream, len);
      stream.write(t.getBytes(), 0, len);
    } else if (obj instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) obj;
      int len = b.getLength();
      WritableUtils.writeVInt(stream, len);
      stream.write(b.getBytes(), 0, len);
    } else {
      buffer.reset();
      obj.write(buffer);
      int length = buffer.getLength();
      WritableUtils.writeVInt(stream, length);
      stream.write(buffer.getData(), 0, length);
    }
  }
  
  /*
  public void setBSPJob(Configuration conf) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_BSPJOB_CONF.code);
    List<String> list = new ArrayList<String>();
    for (Map.Entry<String, String> itm : conf) {
      list.add(itm.getKey());
      list.add(itm.getValue());
    }
    WritableUtils.writeVInt(stream, list.size());
    for (String entry : list) {
      Text.writeString(stream, entry);
    }
    flush();
    LOG.debug("Sent MessageType.SET_BSPJOB_CONF");
  }
*/
  
  /* **************************************************** */
  /* Public Class methods ends...                         */
  /* **************************************************** */

}
