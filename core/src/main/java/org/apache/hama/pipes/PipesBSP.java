package org.apache.hama.pipes;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.bsp.sync.SyncException;

public class PipesBSP<K1, V1, K2, V2 extends Writable, M extends Writable> extends BSP<K1, V1, K2, V2, M> {
	
	private BSPJob job;
	
	public PipesBSP(BSPJob job) {
		super();
		// TODO Auto-generated constructor stub
	}
	/**
	   * This method is your computation method, the main work of your BSP should be
	   * done here.
	   * 
	   * @param peer Your BSPPeer instance.
	   * @throws IOException
	   * @throws SyncException
	   * @throws InterruptedException
	   */
	  public void bsp(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
	      SyncException, InterruptedException  {

	  }
	  /**
	   * This method is called before the BSP method. It can be used for setup
	   * purposes.
	   * 
	   * @param peer Your BSPPeer instance.
	   * @throws IOException
	   */
	  public void setup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
	      SyncException, InterruptedException {

		  peer.get
		  
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
