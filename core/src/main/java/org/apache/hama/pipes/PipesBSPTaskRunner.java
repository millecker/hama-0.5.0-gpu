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
/** MODIFIED FOR GPGPU Usage! **/

package org.apache.hama.pipes;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPTask;
import org.apache.hama.bsp.BSPTaskRunner;
import org.apache.hama.bsp.GroomServer;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.RecordReader;

/**
 * An adaptor to run a C++ BSP Task.
 */
class PipesBSPTaskRunner<K1 extends WritableComparable, V1 extends Writable,
    K2 extends WritableComparable, V2 extends Writable>
    extends BSPTaskRunner {
  
  private BSPJob job;
  private static final Log LOG = LogFactory.getLog(PipesBSPTaskRunner.class.getName());

  public PipesBSPTaskRunner(BSPTask bspTask, GroomServer groom, BSPJob conf) {
	    super(bspTask, groom, conf);
  }
  
  /**
   * Get the new configuration.
   * @param job the job's configuration
   */
  public void configure(BSPJob job) {
    this.job = job;
    //disable the auto increment of the counter. For pipes, no of processed 
    //records could be different(equal or less) than the no of records input.
    
    //SkipBadRecords.setAutoIncrMapperProcCount(job, false);
  }

  /**
   * Run the map task.
   * @param input the set of inputs
   * @param output the object to collect the outputs of the map
   * @param reporter the object to update with status
   */
  /*
  @SuppressWarnings("unchecked")

  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter) throws IOException {
    Application<K1, V1, K2, V2> application = null;
    try {
    
      RecordReader<FloatWritable, NullWritable> fakeInput = 
        (!Submitter.getIsJavaRecordReader(job) && 
         !Submitter.getIsJavaMapper(job)) ? 
	  (RecordReader<FloatWritable, NullWritable>) input : null;
      application = new Application<K1, V1, K2, V2>(job, fakeInput, output, 
                                                    reporter,
          (Class<? extends K2>) job.getOutputKeyClass(), 
          (Class<? extends V2>) job.getOutputValueClass());
    } catch (InterruptedException ie) {
      throw new RuntimeException("interrupted", ie);
    }
    DownwardProtocol<K1, V1> downlink = application.getDownlink();
    boolean isJavaInput = Submitter.getIsJavaRecordReader(job);
    downlink.runMap(reporter.getInputSplit(), 
                    job.getNumReduceTasks(), isJavaInput);
    boolean skipping = job.getBoolean("mapred.skip.on", false);
    try {
      if (isJavaInput) {
        // allocate key & value instances that are re-used for all entries
        K1 key = input.createKey();
        V1 value = input.createValue();
        LOG.info("DEBUG: input = " + input + ", key = " + key + ", value = " + value);
        downlink.setInputTypes(key.getClass().getName(),
                               value.getClass().getName());
        
        while (input.next(key, value)) {
          // map pair to output
          downlink.mapItem(key, value);
          if(skipping) {
            //flushPipesMapRunner.java - DEBUG key value output the streams on every record input if running in skip mode
            //so that we don't buffer other records surrounding a bad record.
            downlink.flush();
          }
        }
        downlink.endOfInput();
      }
      long time = System.currentTimeMillis();
      application.waitForFinish();
      LOG.info("DEBUG: CPUapplication.waitforfinish : " + (System.currentTimeMillis() - time)+" ms");
    } catch (Throwable t) {
      application.abort(t);
    } finally {
      application.cleanup();
    }
    
  }
*/
}
