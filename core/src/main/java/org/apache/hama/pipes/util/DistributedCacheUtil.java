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
package org.apache.hama.pipes.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class DistributedCacheUtil {

  /**
   * Transfers DistributedCache files into the local cache files. Also creates
   * symbolic links for URIs specified with a fragment if
   * DistributedCache.getSymlinks() is true.
   * 
   * @throws IOException If a DistributedCache file cannot be found.
   */
  public static final void moveLocalFiles(Configuration conf) throws IOException {
    StringBuilder files = new StringBuilder();
    boolean first = true;
    if (DistributedCache.getCacheFiles(conf) != null) {
      for (URI uri : DistributedCache.getCacheFiles(conf)) {
        if (uri != null) {
          if (!first) {
            files.append(",");
          }
          if (null != uri.getFragment() && DistributedCache.getSymlink(conf)) {

            FileUtil.symLink(uri.getPath(), uri.getFragment());
            files.append(uri.getFragment()).append(",");
          }
          FileSystem hdfs = FileSystem.get(conf);
          Path pathSrc = new Path(uri.getPath());
          if (hdfs.exists(pathSrc)) {
            LocalFileSystem local = LocalFileSystem.getLocal(conf);
            Path pathDst = new Path(local.getWorkingDirectory(),
                pathSrc.getName());
            hdfs.copyToLocalFile(pathSrc, pathDst);
            files.append(pathDst.toUri().getPath());
          }
          first = false;
        }
      }
    }
    if (files.length() > 0) {
      DistributedCache.addLocalFiles(conf, files.toString());
    }
  }
  
}
