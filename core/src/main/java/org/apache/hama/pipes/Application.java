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

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskLog;

/**
 * This class is responsible for launching and communicating with the child
 * process.
 */
class Application<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable, M extends Writable> {

  private static final Log LOG = LogFactory.getLog(Application.class.getName());
  private ServerSocket serverSocket;
  private Process process;
  private Socket clientSocket;
  private OutputHandler<K2, V2> handler;
  private DownwardProtocol<K1, V1> downlink;
  static final boolean WINDOWS = System.getProperty("os.name").startsWith(
      "Windows");

  /**
   * Start the child process to handle the task for us.
   * 
   * @param conf the task's configuration
   * @param recordReader the fake record reader to update progress with
   * @param output the collector to send output to
   * @param reporter the reporter for the task
   * @param outputKeyClass the class of the output keys
   * @param outputValueClass the class of the output values
   * @throws InterruptedException
   * @throws IOException
   */
  Application(Configuration jobConfig,
      RecordReader<FloatWritable, NullWritable> recordReader,
      OutputCollector<K2, V2> output, Class<? extends K2> outputKeyClass,
      Class<? extends V2> outputValueClass) throws IOException,
      InterruptedException {
    this(jobConfig, recordReader, output, outputKeyClass, outputValueClass,
        false);
  }

  /**
   * Start the child process to handle the task for us.
   * 
   * @param conf the task's configuration
   * @param recordReader the fake record reader to update progress with
   * @param output the collector to send output to
   * @param reporter the reporter for the task
   * @param outputKeyClass the class of the output keys
   * @param outputValueClass the class of the output values
   * @param runOnGPU
   * @throws IOException
   * @throws InterruptedException
   */
  Application(Configuration jobConfig,
      RecordReader<FloatWritable, NullWritable> recordReader,
      OutputCollector<K2, V2> output, Class<? extends K2> outputKeyClass,
      Class<? extends V2> outputValueClass, boolean runOnGPU)
      throws IOException, InterruptedException {
    this(jobConfig, recordReader, output, outputKeyClass, outputValueClass,
        runOnGPU, 0);
  }

  /**
   * Start the child process to handle the task for us.
   * 
   * @param conf the task's configuration
   * @param recordReader the fake record reader to update progress with
   * @param output the collector to send output to
   * @param reporter the reporter for the task
   * @param outputKeyClass the class of the output keys
   * @param outputValueClass the class of the output values
   * @param runOnGPU
   * @throws IOException
   * @throws InterruptedException
   */
  Application(Configuration job,
      RecordReader<FloatWritable, NullWritable> recordReader,
      OutputCollector<K2, V2> output, Class<? extends K2> outputKeyClass,
      Class<? extends V2> outputValueClass, boolean runOnGPU, int GPUDeviceId)
      throws IOException, InterruptedException {

    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    // add TMPDIR environment variable with the value of java.io.tmpdir
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    env.put("hadoop.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));

    List<String> cmd = new ArrayList<String>();
    String interpretor = job.get("hadoop.pipes.executable.interpretor");
    if (interpretor != null) {
      cmd.add(interpretor);
    }

    // Check whether the applicaton will run on GPU and take right executable
    String executable = null;
    try {
      executable = DistributedCache.getLocalCacheFiles(job)[(runOnGPU) ? 1 : 0]
          .toString();
    } catch (Exception e) {
      // if executable (GPU) missing?
      LOG.info("ERROR: "
          + ((Integer.parseInt(e.getMessage()) == 1) ? "GPU " : "CPU")
          + " executable is missing!");
      throw new IOException(((Integer.parseInt(e.getMessage()) == 1) ? "GPU"
          : "CPU") + " executable is missing!");
    }

    if (!new File(executable).canExecute()) {
      // LinuxTaskController sets +x permissions on all distcache files already.
      // In case of DefaultTaskController, set permissions here.
      FileUtil.chmod(executable, "u+x");
    }
    cmd.add(executable);
    // If runOnGPU add GPUDeviceId as parameter for GPUExecutable
    if (runOnGPU)
      // cmd.add(executable + " " + GPUDeviceId);
      cmd.add(Integer.toString(GPUDeviceId));

    // wrap the command in a stdout/stderr capture
    TaskAttemptID taskid = TaskAttemptID.forName(job.get("mapred.task.id"));
    // we are starting map/reduce task of the pipes job. this is not a cleanup
    // attempt.
    File stdout = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDERR);
    long logLength = TaskLog.getTaskLogLength((HamaConfiguration) job);
    cmd = TaskLog.captureOutAndError(null, cmd, stdout, stderr, logLength);
    LOG.info("DEBUG: cmd: " + cmd);

    process = runClient(cmd, env);
    clientSocket = serverSocket.accept();

    handler = new OutputHandler<K2, V2>(output, recordReader);
    K2 outputKey = (K2) ReflectionUtils.newInstance(outputKeyClass, job);
    V2 outputValue = (V2) ReflectionUtils.newInstance(outputValueClass, job);
    downlink = new BinaryProtocol<K1, V1, K2, V2>(clientSocket, handler,
        outputKey, outputValue, job);

    downlink.start();
  }

  private String getSecurityChallenge() {
    Random rand = new Random(System.currentTimeMillis());
    // Use 4 random integers so as to have 16 random bytes.
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(rand.nextInt(0x7fffffff));
    strBuilder.append(rand.nextInt(0x7fffffff));
    strBuilder.append(rand.nextInt(0x7fffffff));
    strBuilder.append(rand.nextInt(0x7fffffff));
    return strBuilder.toString();
  }

  private void writePasswordToLocalFile(String localPasswordFile,
      byte[] password, BSPJob jobConf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(jobConf.getConf());
    Path localPath = new Path(localPasswordFile);
    FSDataOutputStream out = FileSystem.create(localFs, localPath,
        new FsPermission("400"));
    out.write(password);
    out.close();
  }

  /**
   * Get the downward protocol object that can send commands down to the
   * application.
   * 
   * @return the downlink proxy
   */
  DownwardProtocol<K1, V1> getDownlink() {
    return downlink;
  }

  /**
   * Wait for the application to finish
   * 
   * @return did the application finish correctly?
   * @throws Throwable
   */
  boolean waitForFinish() throws Throwable {
    downlink.flush();
    return handler.waitForFinish();
  }

  /**
   * Abort the application and wait for it to finish.
   * 
   * @param t the exception that signalled the problem
   * @throws IOException A wrapper around the exception that was passed in
   */
  void abort(Throwable t) throws IOException {
    LOG.info("Aborting because of " + StringUtils.stringifyException(t));
    try {
      downlink.abort();
      downlink.flush();
    } catch (IOException e) {
      // IGNORE cleanup problems
    }
    try {
      handler.waitForFinish();
    } catch (Throwable ignored) {
      process.destroy();
    }
    IOException wrapper = new IOException("pipe child exception");
    wrapper.initCause(t);
    throw wrapper;
  }

  /**
   * Clean up the child procress and socket.
   * 
   * @throws IOException
   */
  void cleanup() throws IOException {
    serverSocket.close();
    try {
      downlink.close();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Run a given command in a subprocess, including threads to copy its stdout
   * and stderr to our stdout and stderr.
   * 
   * @param command the command and its arguments
   * @param env the environment to run the process in
   * @return a handle on the process
   * @throws IOException
   */
  static Process runClient(List<String> command, Map<String, String> env)
      throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    if (env != null) {
      builder.environment().putAll(env);
    }
    Process result = builder.start();
    return result;
  }

  public static String createDigest(byte[] password, String data)
      throws IOException {
    SecretKey key = JobTokenSecretManager.createSecretKey(password);
    return SecureShuffleUtils.hashFromString(data, key);
  }

}