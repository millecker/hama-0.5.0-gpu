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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.util.KeyValuePair;

public class UplinkReader<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> extends Thread {
  
  private static final Log LOG = LogFactory.getLog(UplinkReader.class);
  
  private DataInputStream inStream;
  private K2 key;
  private V2 value;
  
  private BinaryProtocol<K1, V1, K2, V2> binProtocol;
  private BSPPeer<K1, V1, K2, V2, BytesWritable> peer;
  
  private Map<Integer, SequenceFile.Reader> sequenceFileReaders;
  private Map<Integer, SequenceFile.Writer> sequenceFileWriters;

  public UplinkReader(BinaryProtocol<K1, V1, K2, V2> binaryProtocol,
      BSPPeer<K1, V1, K2, V2, BytesWritable> peer,
      InputStream stream) throws IOException {

    this.binProtocol = binaryProtocol;
    this.peer = peer;
    
    this.inStream = new DataInputStream(new BufferedInputStream(stream,
        BinaryProtocol.BUFFER_SIZE));

    
    this.key = (K2) ReflectionUtils.newInstance((Class<? extends K2>) peer
        .getConfiguration().getClass("bsp.output.key.class", Object.class),
        peer.getConfiguration());

    this.value = (V2) ReflectionUtils.newInstance((Class<? extends V2>) peer
        .getConfiguration().getClass("bsp.output.value.class", Object.class),
        peer.getConfiguration());

    
    this.sequenceFileReaders = new HashMap<Integer, SequenceFile.Reader>();
    this.sequenceFileWriters = new HashMap<Integer, SequenceFile.Writer>();
  }

 

  public void run() {
    DataOutputStream stream = binProtocol.getStream();
    
    while (true) {
      try {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException();
        }

        int cmd = WritableUtils.readVInt(inStream);
        LOG.debug("Handling uplink command " + cmd);

        if (cmd == MessageType.WRITE_KEYVALUE.code) { // INCOMING
          readObject(key); // string or binary only
          readObject(value); // string or binary only
          LOG.debug("Got MessageType.WRITE_KEYVALUE - Key: " + key
              + " Value: " + value);
          peer.write(key, value);

        } else if (cmd == MessageType.READ_KEYVALUE.code) { // OUTGOING

          boolean nullinput = peer.getConfiguration().get(
              "bsp.input.format.class") == null
              || peer.getConfiguration().get("bsp.input.format.class")
                  .equals("org.apache.hama.bsp.NullInputFormat");

          if (!nullinput) {

            KeyValuePair<K1, V1> pair = peer.readNext();

            WritableUtils.writeVInt(stream, MessageType.READ_KEYVALUE.code);
            if (pair != null) {
              binProtocol.writeObject(pair.getKey());
              binProtocol.writeObject(pair.getValue());

              LOG.debug("Responded MessageType.READ_KEYVALUE - Key: "
                  + pair.getKey() + " Value: " + pair.getValue());

            } else {
              Text.writeString(stream, "");
              Text.writeString(stream, "");
              LOG.debug("Responded MessageType.READ_KEYVALUE - EMPTY KeyValue Pair");
            }
            binProtocol.flush();

          } else {
            /* TODO */
            /* Send empty Strings to show no KeyValue pair is available */
            WritableUtils.writeVInt(stream, MessageType.READ_KEYVALUE.code);
            Text.writeString(stream, "");
            Text.writeString(stream, "");
            binProtocol.flush();
            LOG.debug("Responded MessageType.READ_KEYVALUE - EMPTY KeyValue Pair");
          }

        } else if (cmd == MessageType.INCREMENT_COUNTER.code) { // INCOMING
          // int id = WritableUtils.readVInt(inStream);
          String group = Text.readString(inStream);
          String name = Text.readString(inStream);
          long amount = WritableUtils.readVLong(inStream);
          peer.incrementCounter(name, group, amount);

        } else if (cmd == MessageType.REGISTER_COUNTER.code) { // INCOMING
          /* TODO */
          /*
           * Is not used in HAMA -> Hadoop Pipes - maybe for performance, skip
           * transferring group and name each INCREMENT
           */

        } else if (cmd == MessageType.TASK_DONE.code) { // INCOMING
          LOG.debug("Got MessageType.TASK_DONE");
          binProtocol.setHasTask(false);

        } else if (cmd == MessageType.DONE.code) { // INCOMING
          LOG.debug("Pipe child done");
          return;

        } else if (cmd == MessageType.SEND_MSG.code) { // INCOMING
          String peerName = Text.readString(inStream);
          BytesWritable msg = new BytesWritable();
          readObject(msg);
          LOG.debug("Got MessageType.SEND_MSG to peerName: " + peerName);
          peer.send(peerName, msg);

        } else if (cmd == MessageType.GET_MSG_COUNT.code) { // OUTGOING
          WritableUtils.writeVInt(stream, MessageType.GET_MSG_COUNT.code);
          WritableUtils.writeVInt(stream, peer.getNumCurrentMessages());
          binProtocol.flush();
          LOG.debug("Responded MessageType.GET_MSG_COUNT - Count: "
              + peer.getNumCurrentMessages());

        } else if (cmd == MessageType.GET_MSG.code) { // OUTGOING
          LOG.debug("Got MessageType.GET_MSG");
          WritableUtils.writeVInt(stream, MessageType.GET_MSG.code);
          BytesWritable msg = peer.getCurrentMessage();
          if (msg != null)
            binProtocol.writeObject(msg);

          binProtocol.flush();
          LOG.debug("Responded MessageType.GET_MSG - Message(BytesWritable) ");// +msg);

        } else if (cmd == MessageType.SYNC.code) { // INCOMING
          LOG.debug("Got MessageType.SYNC");
          peer.sync(); // this call blocks

        } else if (cmd == MessageType.GET_ALL_PEERNAME.code) { // OUTGOING
          LOG.debug("Got MessageType.GET_ALL_PEERNAME");
          WritableUtils.writeVInt(stream, MessageType.GET_ALL_PEERNAME.code);
          WritableUtils.writeVInt(stream, peer.getAllPeerNames().length);
          for (String s : peer.getAllPeerNames())
            Text.writeString(stream, s);

          binProtocol.flush();
          LOG.debug("Responded MessageType.GET_ALL_PEERNAME - peerNamesCount: "
              + peer.getAllPeerNames().length);

        } else if (cmd == MessageType.GET_PEERNAME.code) { // OUTGOING
          int id = WritableUtils.readVInt(inStream);
          LOG.debug("Got MessageType.GET_PEERNAME id: " + id);

          WritableUtils.writeVInt(stream, MessageType.GET_PEERNAME.code);
          if (id == -1) { // -1 indicates get own PeerName
            Text.writeString(stream, peer.getPeerName());
            LOG.debug("Responded MessageType.GET_PEERNAME - Get Own PeerName: "
                + peer.getPeerName());

          } else if ((id < -1) || (id >= peer.getNumPeers())) {
            // if no PeerName for this index is found write emptyString
            Text.writeString(stream, "");
            LOG.debug("Responded MessageType.GET_PEERNAME - Empty PeerName!");

          } else {
            Text.writeString(stream, peer.getPeerName(id));
            LOG.debug("Responded MessageType.GET_PEERNAME - PeerName: "
                + peer.getPeerName(id));
          }
          binProtocol.flush();

        } else if (cmd == MessageType.GET_PEER_INDEX.code) { // OUTGOING
          WritableUtils.writeVInt(stream, MessageType.GET_PEER_INDEX.code);
          WritableUtils.writeVInt(stream, peer.getPeerIndex());
          binProtocol.flush();
          LOG.debug("Responded MessageType.GET_PEER_INDEX - PeerIndex: "
              + peer.getPeerIndex());

        } else if (cmd == MessageType.GET_PEER_COUNT.code) { // OUTGOING
          WritableUtils.writeVInt(stream, MessageType.GET_PEER_COUNT.code);
          WritableUtils.writeVInt(stream, peer.getNumPeers());
          binProtocol.flush();
          LOG.debug("Responded MessageType.GET_PEER_COUNT - NumPeers: "
              + peer.getNumPeers());

        } else if (cmd == MessageType.GET_SUPERSTEP_COUNT.code) { // OUTGOING
          WritableUtils.writeVInt(stream,
              MessageType.GET_SUPERSTEP_COUNT.code);
          WritableUtils.writeVLong(stream, peer.getSuperstepCount());
          binProtocol.flush();
          LOG.debug("Responded MessageType.GET_SUPERSTEP_COUNT - SuperstepCount: "
              + peer.getSuperstepCount());

        } else if (cmd == MessageType.REOPEN_INPUT.code) { // INCOMING
          LOG.debug("Got MessageType.REOPEN_INPUT");
          peer.reopenInput();

        } else if (cmd == MessageType.CLEAR.code) { // INCOMING
          LOG.debug("Got MessageType.CLEAR");
          peer.clear();

          /* SequenceFileConnector Implementation */
        } else if (cmd == MessageType.SEQFILE_OPEN.code) { // OUTGOING
          String path = Text.readString(inStream);
          // option - read = "r" or write = "w"
          String option = Text.readString(inStream);

          int fileID = -1;

          Configuration conf = peer.getConfiguration();
          FileSystem fs = FileSystem.get(conf);
          if (path.equals("r")) {
            SequenceFile.Reader reader;
            try {
              reader = new SequenceFile.Reader(fs, new Path(path), conf);
              fileID = reader.hashCode();
              sequenceFileReaders.put(fileID, reader);
            } catch (IOException e) {
              fileID = -1;
            }

          } else if (path.equals("w")) {
            SequenceFile.Writer writer;
            try {
              writer = new SequenceFile.Writer(fs, conf, new Path(path),
                  Text.class, Text.class);
              fileID = writer.hashCode();
              sequenceFileWriters.put(fileID, writer);
            } catch (IOException e) {
              fileID = -1;
            }
          }

          WritableUtils.writeVInt(stream, MessageType.SEQFILE_OPEN.code);
          WritableUtils.writeVInt(stream, fileID);
          binProtocol.flush();
          LOG.debug("Responded MessageType.SEQFILE_OPEN - FileID: " + fileID);

        } else if (cmd == MessageType.SEQFILE_READNEXT.code) { // OUTGOING
          int fileID = WritableUtils.readVInt(inStream);

          Writable key = null;
          Writable val = null;

          if (sequenceFileReaders.containsKey(fileID))
            sequenceFileReaders.get(fileID).next(key, val);
          
          // RESPOND
          WritableUtils.writeVInt(stream, MessageType.SEQFILE_READNEXT.code);
          if (key != null && val != null) {
            Text.writeString(stream, key.toString());
            Text.writeString(stream, val.toString());
            LOG.debug("Responded MessageType.SEQFILE_READNEXT - key: " + key
                + " val: " + val);
          } else {
            Text.writeString(stream, "");
            Text.writeString(stream, "");
            LOG.debug("Responded MessageType.SEQFILE_READNEXT - EMPTY KeyValue Pair");
          }
          binProtocol.flush();

        } else if (cmd == MessageType.SEQFILE_APPEND.code) { // INCOMING
          int fileID = WritableUtils.readVInt(inStream);
          String keyStr = Text.readString(inStream);
          String valueStr = Text.readString(inStream);

          boolean result = false;
          if (sequenceFileWriters.containsKey(fileID)) {
            sequenceFileWriters.get(fileID).append(new Text(keyStr),
                new Text(valueStr));
            result = true;
          }

          // RESPOND
          WritableUtils.writeVInt(stream, MessageType.SEQFILE_APPEND.code);
          WritableUtils.writeVInt(stream, result ? 1 : 0);
          binProtocol.flush();
          LOG.debug("Responded MessageType.SEQFILE_APPEND - Result: "
              + result);

        } else if (cmd == MessageType.SEQFILE_CLOSE.code) { // OUTGOING
          int fileID = WritableUtils.readVInt(inStream);
          boolean result = false;

          if (sequenceFileReaders.containsKey(fileID)) {
            sequenceFileReaders.get(fileID).close();
            result = true;
          } else if (sequenceFileWriters.containsKey(fileID)) {
            sequenceFileWriters.get(fileID).close();
            result = true;
          }

          // RESPOND
          WritableUtils.writeVInt(stream, MessageType.SEQFILE_CLOSE.code);
          WritableUtils.writeVInt(stream, result ? 1 : 0);
          binProtocol.flush();
          LOG.debug("Responded MessageType.SEQFILE_CLOSE - Result: " + result);

        } else {
          throw new IOException("Bad command code: " + cmd);
        }

      } catch (InterruptedException e) {
        return;
      } catch (Throwable e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
  }
 
  private void readObject(Writable obj) throws IOException {
    int numBytes = WritableUtils.readVInt(inStream);
    byte[] buffer;
    // For BytesWritable and Text, use the specified length to set the length
    // this causes the "obvious" translations to work. So that if you emit
    // a string "abc" from C++, it shows up as "abc".
    if (obj instanceof BytesWritable) {
      buffer = new byte[numBytes];
      inStream.readFully(buffer);
      ((BytesWritable) obj).set(buffer, 0, numBytes);
    } else if (obj instanceof Text) {
      buffer = new byte[numBytes];
      inStream.readFully(buffer);
      ((Text) obj).set(buffer);
    } else if (obj instanceof NullWritable) {
      throw new IOException(
          "Cannot read data into NullWritable! Check OutputClasses!");
    } else {
      /* TODO */
      /* IntWritable, DoubleWritable */
      throw new IOException(
          "Hama Pipes does only support Text as Key/Value output!");
      // obj.readFields(inStream);
    }
  }
  
  
  public void closeConnection() throws IOException {
    inStream.close();
  }
}
