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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
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

  private Application<K1, V1, K2, V2, BytesWritable> application; 
	
  
  /**
   * Transfers DistributedCache files into the local cache files. Also creates
   * symbolic links for URIs specified with a fragment if 
   * DistributedCache.getSymlinks() is true.
   * 
   * @param configuration 
   *          The configuration file with non-local DistributedCache
   *          attached.
   * @return 
   *          The configuration file with local variants for the 
   *          DistributedCache attached. 
   * @throws IOException 
   *          If a DistributedCache file cannot be found.
   */
  public Configuration moveLocalFiles(BSPPeer<K1, V1, K2, V2, BytesWritable> peer)
      throws IOException {	
	Configuration configuration = peer.getConfiguration();
	StringBuilder files = new StringBuilder();
  	boolean first = true;
  	if (DistributedCache.getCacheFiles(configuration)!=null) {
  		for (URI uri : DistributedCache.getCacheFiles(configuration)) {
  			if (uri!=null) {
  				if (!first) {
  					files.append(",");
  				}
	  			if (null != uri.getFragment()
	  				&& DistributedCache.getSymlink(configuration)) {
	  			
	  				FileUtil.symLink(uri.getPath(), uri.getFragment());
					files.append(uri.getFragment()).append(",");
				}
	  			FileSystem hdfs = FileSystem.get(configuration);
	  			Path pathSrc = new Path(uri.getPath());
	  			if(hdfs.exists(pathSrc)) {
	  				LocalFileSystem local = LocalFileSystem.getLocal(configuration);
	  				Path pathDst = new Path(local.getWorkingDirectory(),pathSrc.getName());
	  				hdfs.copyToLocalFile(pathSrc, pathDst);
	  				files.append(pathDst.toUri().getPath());
	  			}
            	first = false;
          	}
  		}
  	}
    if (files.length()>0) {
       DistributedCache.addLocalFiles(configuration, files.toString());
    }
    return configuration;
  }
  
  /**
   * Deletes the local cache files.
   * 
   * @param configuration 
   *          The configuration file with local variants for the 
   *          DistributedCache attached. 
   * @throws IOException 
   *          If a DistributedCache file cannot be found.
   */
  public void deleteLocalFiles(BSPPeer<K1, V1, K2, V2, BytesWritable> peer)
      throws IOException {	
	Configuration configuration = peer.getConfiguration();
  	if (DistributedCache.getLocalCacheFiles(configuration)!=null) {
  		for (Path path : DistributedCache.getLocalCacheFiles(configuration)) {
  			if (path!=null) {
  				LocalFileSystem local = LocalFileSystem.getLocal(configuration);
	  			if(local.exists(path)) {
	  				local.delete(path,true); //recursive true
	  			}
          	}
  		}
  	}
  	DistributedCache.setLocalFiles(configuration, "");
  }
  
  
  public void setup(BSPPeer<K1, V1, K2, V2, BytesWritable> peer) throws IOException,
      SyncException, InterruptedException {
	    
	// TODO Workaround for DistributedCache bug in Hama
	moveLocalFiles(peer);
	  
    this.application = new Application<K1, V1, K2, V2, BytesWritable>(peer);
    
    application.getDownlink().runSetup(false, false);
    
    try {
		application.waitForFinish();
	} catch (Throwable e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  
  public void bsp(BSPPeer<K1, V1, K2, V2, BytesWritable> peer) throws IOException,
      SyncException, InterruptedException {

	application.getDownlink().runBsp(false, false);
	    
	try {
		application.waitForFinish();
	} catch (Throwable e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
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
  public void cleanup(BSPPeer<K1, V1, K2, V2, BytesWritable> peer) throws IOException {
	  
	  // TODO Workaround for DistributedCache bug in Hama
	  deleteLocalFiles(peer);
	  
	  application.getDownlink().runCleanup(false, false);
	  
	  try {
			application.waitForFinish();
	  } catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	  }
	  
	  application.cleanup();
  }

}
