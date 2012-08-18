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
package org.apache.hama.pipes;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

/**
 * 
 * @author Martin Illecker
 * 
 */
public class PipesBSP<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable, M extends Writable>
    extends BSP<K1, V1, K2, V2, BytesWritable> {

  private static final Log LOG = LogFactory.getLog(PipesBSP.class);
  private Application<K1, V1, K2, V2, BytesWritable> application;

  public void setup(BSPPeer<K1, V1, K2, V2, BytesWritable> peer)
      throws IOException, SyncException, InterruptedException {

    this.application = new Application<K1, V1, K2, V2, BytesWritable>(peer);

    application.getDownlink().runSetup(false, false);

    try {
      application.waitForFinish();
    } catch (Throwable e) {
      LOG.error(e);
    }
  }

  public void bsp(BSPPeer<K1, V1, K2, V2, BytesWritable> peer)
      throws IOException, SyncException, InterruptedException {

    application.getDownlink().runBsp(false, false);

    try {
      application.waitForFinish();
    } catch (Throwable e) {
      LOG.error(e);
    }
  }

  /**
   * This method is called after the BSP method. It can be used for cleanup
   * purposes. Cleanup is guranteed to be called after the BSP runs, even in
   * case of exceptions.
   * 
   * @param peer Your BSPPeer instance.
   * @throws IOException
   */
  public void cleanup(BSPPeer<K1, V1, K2, V2, BytesWritable> peer)
      throws IOException {

    application.getDownlink().runCleanup(false, false);

    try {
      application.waitForFinish();
    } catch (Throwable e) {
      LOG.error(e);
    }
    application.cleanup();
  }

}
