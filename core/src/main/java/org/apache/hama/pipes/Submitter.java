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
//BSPJob <--> JobConf

package org.apache.hama.pipes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.OutputFormat;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.util.GenericOptionsParser;

/**
 * The main entry point and job submitter. It may either be used as a command
 * line-based or API-based method to launch Pipes jobs.
 * 
 * Adapted from Hadoop Pipes
 * 
 * @author Martin Illecker
 * 
 */
public class Submitter implements Tool {

  protected static final Log LOG = LogFactory.getLog(Submitter.class);
  private HamaConfiguration conf;

  public Submitter() {
    this.conf = new HamaConfiguration();
  }

  public Submitter(HamaConfiguration conf) {
    setConf(conf);
  }

  @Override
  public HamaConfiguration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = (HamaConfiguration) conf;
  }

  /**
   * Get the URI of the CPU application's executable.
   * 
   * @param conf
   * @return the URI where the application's executable is located
   */
  public static String getExecutable(Configuration conf) {
    return conf.get("hama.pipes.executable");
  }

  /**
   * Set the URI for the CPU application's executable. Normally this is a hdfs:
   * location.
   * 
   * @param conf
   * @param executable The URI of the application's executable.
   */
  public static void setExecutable(Configuration conf, String executable) {
    conf.set("hama.pipes.executable", executable);
  }

  public static String getCPUExecutable(Configuration conf) {
    return getExecutable(conf);
  }

  public static void setCPUExecutable(Configuration conf, String executable) {
    setExecutable(conf, executable);
  }

  /**
   * Set the URI for the GPU application's executable. Normally this is a hdfs:
   * location.
   * 
   * @param conf
   * @param executable The URI of the application's executable.
   */
  public static void setGPUExecutable(Configuration conf, String executable) {
    conf.set("hama.pipes.gpu.executable", executable);
  }

  /**
   * Get the URI of the GPU application's executable.
   * 
   * @param conf
   * @return the URI where the application's executable is located
   */
  public static String getGPUExecutable(Configuration conf) {
    return conf.get("hama.pipes.gpu.executable");
  }

  /**
   * Set whether the BSP is written in Java.
   * 
   * @param conf the configuration to modify
   * @param value the new value
   */
  public static void setIsJavaBSP(Configuration conf, boolean value) {
    conf.setBoolean("hama.pipes.java.bsp", value);
  }

  /**
   * Check whether the job is using a Java BSP.
   * 
   * @param conf the configuration to check
   * @return is it a Java BSP?
   */
  public static boolean getIsJavaBSP(Configuration conf) {
    return conf.getBoolean("hama.pipes.java.bsp", false);
  }

  /**
   * Set whether the job is using a Java RecordReader.
   * 
   * @param conf the configuration to modify
   * @param value the new value
   */
  public static void setIsJavaRecordReader(Configuration conf, boolean value) {
    conf.setBoolean("hama.pipes.java.recordreader", value);
  }

  /**
   * Check whether the job is using a Java RecordReader
   * 
   * @param conf the configuration to check
   * @return is it a Java RecordReader?
   */
  public static boolean getIsJavaRecordReader(Configuration conf) {
    return conf.getBoolean("hama.pipes.java.recordreader", false);
  }

  /**
   * Set whether the job will use a Java RecordWriter.
   * 
   * @param conf the configuration to modify
   * @param value the new value to set
   */
  public static void setIsJavaRecordWriter(Configuration conf, boolean value) {
    conf.setBoolean("hama.pipes.java.recordwriter", value);
  }

  /**
   * Will the reduce use a Java RecordWriter?
   * 
   * @param conf the configuration to check
   * @return true, if the output of the job will be written by Java
   */
  public static boolean getIsJavaRecordWriter(Configuration conf) {
    return conf.getBoolean("hama.pipes.java.recordwriter", false);
  }

  /**
   * Set the configuration, if it doesn't already have a value for the given
   * key.
   * 
   * @param conf the configuration to modify
   * @param key the key to set
   * @param value the new "default" value to set
   */
  private static void setIfUnset(Configuration conf, String key, String value) {
    if (conf.get(key) == null) {
      conf.set(key, value);
    }
  }

  /**
   * Save away the user's original partitioner before we override it.
   * 
   * @param conf the configuration to modify
   * @param cls the user's partitioner class
   */
  static void setJavaPartitioner(Configuration conf, Class cls) {
    conf.set("hama.pipes.partitioner", cls.getName());
  }

  /**
   * Get the user's original partitioner.
   * 
   * @param conf the configuration to look in
   * @return the class that the user submitted
   */
  static Class<? extends Partitioner> getJavaPartitioner(Configuration conf) {
    return conf.getClass("hama.pipes.partitioner", HashPartitioner.class,
        Partitioner.class);
  }

  /**
   * Does the user want to keep the command file for debugging? If this is true,
   * pipes will write a copy of the command data to a file in the task directory
   * named "downlink.data", which may be used to run the C++ program under the
   * debugger. You probably also want to set
   * JobConf.setKeepFailedTaskFiles(true) to keep the entire directory from
   * being deleted. To run using the data file, set the environment variable
   * "hadoop.pipes.command.file" to point to the file.
   * 
   * @param conf the configuration to check
   * @return will the framework save the command file?
   */
  public static boolean getKeepCommandFile(Configuration conf) {
    return conf.getBoolean("hama.pipes.command-file.keep", false);
  }

  /**
   * Set whether to keep the command file for debugging
   * 
   * @param conf the configuration to modify
   * @param keep the new value
   */
  public static void setKeepCommandFile(Configuration conf, boolean keep) {
    conf.setBoolean("hama.pipes.command-file.keep", keep);
  }

  /**
   * Submit a job to the cluster. All of the necessary modifications to the job
   * to run under pipes are made to the configuration.
   * 
   * @param conf the job to submit to the cluster (MODIFIED)
   * @throws IOException
   */
  public static void runJob(BSPJob job) throws IOException {
    setupPipesJob(job);
    BSPJobClient.runJob(job);
  }

  private static void setupPipesJob(BSPJob job) throws IOException {
    // default map output types to Text
    if (!getIsJavaBSP(job.getConf())) {
      job.setBspClass(PipesBSP.class);
      job.setJarByClass(PipesBSP.class);

      // Save the user's partitioner and hook in our's.
      // setJavaPartitioner(job, job.getPartitionerClass());
      // job.setPartitionerClass(PipesPartitioner.class);
    }
    /*
     * if (!getIsJavaReducer(conf)) { conf.setReducerClass(PipesReducer.class);
     * if (!getIsJavaRecordWriter(conf)) {
     * conf.setOutputFormat(NullOutputFormat.class); } }
     */

    String textClassname = Text.class.getName();
    setIfUnset(job.getConf(), "bsp.input.key.class", textClassname);
    setIfUnset(job.getConf(), "bsp.input.value.class", textClassname);
    setIfUnset(job.getConf(), "bsp.output.key.class", textClassname);
    setIfUnset(job.getConf(), "bsp.output.value.class", textClassname);

    // TODO Set default Job name
    setIfUnset(job.getConf(), "bsp.job.name", "Hama Pipes Job");

    LOG.info("DEBUG: isJavaRecordReader: "
        + getIsJavaRecordReader(job.getConf()));
    LOG.info("DEBUG: BspClass: " + job.getBspClass().getName());
    // conf.setInputFormat(NLineInputFormat.class);
    LOG.info("DEBUG: InputFormat: " + job.getInputFormat());
    LOG.info("DEBUG: InputKeyClass: " + job.getInputKeyClass().getName());
    LOG.info("DEBUG: InputValueClass: " + job.getInputValueClass().getName());
    LOG.info("DEBUG: OutputKeyClass: " + job.getOutputKeyClass().getName());
    LOG.info("DEBUG: OutputValueClass: " + job.getOutputValueClass().getName());

    // Use PipesNonJavaInputFormat if necessary to handle progress reporting
    // from C++ RecordReaders ...
    /*
     * if (!getIsJavaRecordReader(job) && !getIsJavaMapper(job)) {
     * job.setClass("mapred.pipes.user.inputformat",
     * job.getInputFormat().getClass(), InputFormat.class);
     * job.setInputFormat(PipesNonJavaInputFormat.class); }
     */

    LOG.info("DEBUG: bsp.master.address: "
        + job.getConf().get("bsp.master.address"));
    LOG.info("DEBUG: bsp.local.tasks.maximum: "
        + job.getConf().get("bsp.local.tasks.maximum"));
    LOG.info("DEBUG: fs.default.name: " + job.getConf().get("fs.default.name"));

    // String exec = getExecutable(conf);
    String cpubin = getCPUExecutable(job.getConf());
    String gpubin = getGPUExecutable(job.getConf());
    LOG.info("DEBUG: CPUbin = '" + cpubin + "'");
    LOG.info("DEBUG: GPUbin = '" + gpubin + "'");
    // if (exec == null) {
    if (cpubin == null && gpubin == null) {
      throw new IllegalArgumentException("No application program defined.");
    }
    // add default debug script only when executable is expressed as
    // <path>#<executable>
    // if (exec.contains("#")) {
    /*
     * if (cpubin!=null && cpubin.contains("#") || gpubin!=null &&
     * gpubin.contains("#")) { DistributedCache.createSymlink(job.getConf()); //
     * set default gdb commands for map and reduce task String defScript =
     * "$HADOOP_HOME/src/c++/pipes/debug/pipes-default-script";
     * setIfUnset(job,"mapred.map.task.debug.script",defScript);
     * setIfUnset(job,"mapred.reduce.task.debug.script",defScript); }
     */

    URI[] fileCache = DistributedCache.getCacheFiles(job.getConf());
    int count = ((cpubin != null) && (gpubin != null)) ? 2 : 1;
    if (fileCache == null) {
      fileCache = new URI[count];
    } else {
      URI[] tmp = new URI[fileCache.length + count];
      System.arraycopy(fileCache, 0, tmp, count, fileCache.length);
      fileCache = tmp;
    }

    if (cpubin != null) {
      try {
        fileCache[0] = new URI(cpubin);
      } catch (URISyntaxException e) {
        IOException ie = new IOException("Problem parsing execable URI "
            + cpubin);
        ie.initCause(e);
        throw ie;
      }
    }
    if (gpubin != null) {
      try {
        fileCache[1] = new URI(gpubin);
      } catch (URISyntaxException e) {
        IOException ie = new IOException("Problem parsing execable URI "
            + gpubin);
        ie.initCause(e);
        throw ie;
      }
    }
    DistributedCache.setCacheFiles(fileCache, job.getConf());
  }

  /**
   * A command line parser for the CLI-based Pipes job submitter.
   */
  static class CommandLineParser {
    private Options options = new Options();

    void addOption(String longName, boolean required, String description,
        String paramName) {
      Option option = OptionBuilder.withArgName(paramName).hasArgs(1)
          .withDescription(description).isRequired(required).create(longName);
      options.addOption(option);
    }

    void addArgument(String name, boolean required, String description) {
      Option option = OptionBuilder.withArgName(name).hasArgs(1)
          .withDescription(description).isRequired(required).create();
      options.addOption(option);

    }

    Parser createParser() {
      Parser result = new BasicParser();
      return result;
    }

    void printUsage() {
      // The CLI package should do this for us, but I can't figure out how
      // to make it print something reasonable.
      System.out.println("bin/hama pipes");
      System.out.println("  [-input <path>] // Input directory");
      System.out.println("  [-output <path>] // Output directory");
      System.out.println("  [-jar <jar file> // jar filename");
      System.out.println("  [-inputformat <class>] // InputFormat class");
      System.out.println("  [-bsp <class>] // Java Map class");
      System.out.println("  [-partitioner <class>] // Java Partitioner");
      System.out.println("  [-combiner <class>] // Java Combiner class");
      System.out.println("  [-output <class>] // Java RecordWriter");
      System.out.println("  [-program <executable>] // executable URI");
      System.out
          .println("  [-cpubin <executable>] //URI to application cpu executable");
      System.out
          .println("  [-gpubin <executable>] //URI to application gpu executable");
      System.out.println();
      GenericOptionsParser.printGenericCommandUsage(System.out);
    }
  }

  private static <InterfaceType> Class<? extends InterfaceType> getClass(
      CommandLine cl, String key, HamaConfiguration conf,
      Class<InterfaceType> cls) throws ClassNotFoundException {

    return conf.getClassByName((String) cl.getOptionValue(key)).asSubclass(cls);
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLineParser cli = new CommandLineParser();
    if (args.length == 0) {
      cli.printUsage();
      return 1;
    }

    LOG.info("DEBUG: Hama pipes Submitter started!");

    cli.addOption("input", false, "input path for bsp", "path");
    cli.addOption("output", false, "output path from bsp", "path");

    cli.addOption("jar", false, "job jar file", "path");
    cli.addOption("inputformat", false, "java classname of InputFormat",
        "class");
    // cli.addArgument("javareader", false, "is the RecordReader in Java");

    cli.addOption("bsp", false, "java classname of bsp", "class");
    cli.addOption("partitioner", false, "java classname of Partitioner",
        "class");
    cli.addOption("outputformat", false, "java classname of OutputFormat",
        "class");

    cli.addOption(
        "jobconf",
        false,
        "\"n1=v1,n2=v2,..\" (Deprecated) Optional. Add or override a JobConf property.",
        "key=val");

    cli.addOption("program", false, "URI to application executable", "class");
    cli.addOption("cpubin", false, "URI to application cpu executable", "class");
    cli.addOption("gpubin", false, "URI to application gpu executable", "class");
    Parser parser = cli.createParser();
    try {

      // check generic arguments -conf
      LOG.debug("DEBUG: execute GenericOptionsParser");
      GenericOptionsParser genericParser = new GenericOptionsParser(getConf(),
          args);
      // get other arguments
      CommandLine results = parser.parse(cli.options,
          genericParser.getRemainingArgs());
      LOG.debug("DEBUG: NormalArguments: " + Arrays.toString(results.getArgs()));

      BSPJob job = new BSPJob(getConf());

      if (results.hasOption("input")) {
        FileInputFormat.setInputPaths(job,
            (String) results.getOptionValue("input"));
      }
      if (results.hasOption("output")) {
        FileOutputFormat.setOutputPath(job,
            new Path((String) results.getOptionValue("output")));
      }
      if (results.hasOption("jar")) {
        job.setJar((String) results.getOptionValue("jar"));
      }

      if (results.hasOption("inputformat")) {
        setIsJavaRecordReader(job.getConf(), true);
        job.setInputFormat(getClass(results, "inputformat", conf,
            InputFormat.class));
      }
      if (results.hasOption("outputformat")) {
        setIsJavaRecordWriter(job.getConf(), true);
        job.setOutputFormat(getClass(results, "outputformat", conf,
            OutputFormat.class));
      }

      if (results.hasOption("bsp")) {
        setIsJavaBSP(job.getConf(), true);
        job.setBspClass(getClass(results, "bsp", conf, BSP.class));
      }
      /*
       * if (results.hasOption("partitioner")) {
       * job.setPartitionerClass(getClass(results, "partitioner", conf,
       * Partitioner.class)); } if (results.hasOption("combiner")) {
       * //setIsJavaReducer(job, true); job.setCombinerClass(getClass(results,
       * "combiner", conf, Combiner.class)); }
       */

      if (results.hasOption("jobconf")) {
        LOG.warn("-jobconf option is deprecated, please use -D instead.");
        String options = (String) results.getOptionValue("jobconf");
        StringTokenizer tokenizer = new StringTokenizer(options, ",");
        while (tokenizer.hasMoreTokens()) {
          String keyVal = tokenizer.nextToken().trim();
          String[] keyValSplit = keyVal.split("=", 2);
          job.set(keyValSplit[0], keyValSplit[1]);
        }
      }

      if (results.hasOption("program")) {
        setExecutable(job.getConf(), (String) results.getOptionValue("program"));
      }
      if (results.hasOption("cpubin")) {
        setCPUExecutable(job.getConf(),
            (String) results.getOptionValue("cpubin"));
      }
      if (results.hasOption("gpubin")) {
        setGPUExecutable(job.getConf(),
            (String) results.getOptionValue("gpubin"));
      }

      // if they gave us a jar file, include it into the class path
      String jarFile = job.getJar();
      if (jarFile != null) {
        final URL[] urls = new URL[] { FileSystem.getLocal(conf)
            .pathToFile(new Path(jarFile)).toURL() };
        // FindBugs complains that creating a URLClassLoader should be
        // in a doPrivileged() block.
        ClassLoader loader = AccessController
            .doPrivileged(new PrivilegedAction<ClassLoader>() {
              public ClassLoader run() {
                return new URLClassLoader(urls);
              }
            });
        conf.setClassLoader(loader);
      }

      runJob(job);
      return 0;
    } catch (ParseException pe) {
      LOG.info("Error : " + pe);
      cli.printUsage();
      return 1;
    }

  }

  /**
   * Submit a pipes job based on the command line arguments.
   * 
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int exitCode = new Submitter().run(args);
    System.exit(exitCode);
  }

}
