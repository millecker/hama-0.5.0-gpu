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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.bsp.sync.SyncException;

public class PipesBSP<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable, M extends Writable>
    extends BSP<K1, V1, K2, V2, M> {

  public void setup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      SyncException, InterruptedException {

    Application<K1, V1, K2, V2, M> application = null;
    
    
  }

  public void bsp(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      SyncException, InterruptedException {

  }

  /**
   * This method is called after the BSP method. It can be used for cleanup
   * purposes. Cleanup is guranteed to be called after the BSP runs, even in
   * case of exceptions.
   * 
   * @param peer Your BSPPeer instance.
   * @throws IOException
   */
  public void cleanup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException {

  }

}
