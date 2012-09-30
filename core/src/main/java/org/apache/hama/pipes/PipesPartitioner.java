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

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.Partitioner;

/**
 * 
 * @author Martin Illecker
 * 
 *         PipesPartitioner is a Wrapper for C++ Partitioner Java Partitioner ->
 *         BinaryProtocol -> C++ Partitioner and back
 */
public class PipesPartitioner<K, V> implements Partitioner<K, V>,
    PipesApplicable {

  private PipesApplication<? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable> pipesApp = null;

  /**
   * Partitions a specific key value mapping to a bucket.
   * 
   * @param key
   * @param value
   * @param numTasks
   * @return a number between 0 and numTasks (exclusive) that tells which
   *         partition it belongs to.
   */
  @Override
  public int getPartition(K key, V value, int numTasks) {
    return 0;
  }

  @Override
  public void setApplication(
      PipesApplication<? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable> pipesApp) {

    this.pipesApp = pipesApp;
  }

}
