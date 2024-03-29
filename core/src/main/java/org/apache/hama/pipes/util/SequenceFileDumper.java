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

import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.GenericOptionsParser;

public class SequenceFileDumper {

  protected static final Log LOG = LogFactory.getLog(SequenceFileDumper.class);
  public static String LINE_SEP = System.getProperty("line.separator");

  private SequenceFileDumper() {
  }

  /**
   * A command line parser for the CLI-based SequenceFileDumper.
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
      System.out.println("hama seqdumper");
      System.out
          .println("  [-seqFile <path>] // The Sequence File containing the Clusters");
      System.out
          .println("  [-output <path>] // The output file.  If not specified, dumps to the console");
      System.out
          .println("  [-substring <number> // The number of chars of the FormatString() to print");
      System.out.println("  [-count <true>] // Report the count only");
      System.out.println("  [-help] // Print out help");
      System.out.println();
      GenericOptionsParser.printGenericCommandUsage(System.out);
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser cli = new CommandLineParser();
    if (args.length == 0) {
      cli.printUsage();
      return;
    }

    LOG.info("DEBUG: Hama SequenceFileDumper started!");

    cli.addOption("seqFile", false,
        "The Sequence File containing the Clusters", "path");
    cli.addOption("output", false,
        "The output file.  If not specified, dumps to the console", "path");

    cli.addOption("substring", false,
        "The number of chars of the FormatString() to print", "number");
    cli.addOption("count", false, "Report the count only", "number");
    cli.addOption("help", false, "Print out help", "class");

    Parser parser = cli.createParser();

    try {
      HamaConfiguration conf = new HamaConfiguration();
      
      GenericOptionsParser genericParser = new GenericOptionsParser(conf,
          args);
      
      CommandLine cmdLine = parser.parse(cli.options, genericParser.getRemainingArgs());
      LOG.debug("DEBUG: Arguments: " + genericParser.getRemainingArgs());

      if (cmdLine.hasOption("help")) {
        cli.printUsage();
        return;
      }

      if (cmdLine.hasOption("seqFile")) {
        Path path = new Path(cmdLine.getOptionValue("seqFile"));  

        FileSystem fs = FileSystem.get(path.toUri(), conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        Writer writer;
        if (cmdLine.hasOption("output")) {
          writer = new FileWriter(cmdLine.getOptionValue("output"));
        } else {
          writer = new OutputStreamWriter(System.out);
        }
        writer.append("Input Path: ").append(String.valueOf(path))
            .append(LINE_SEP);

        int sub = Integer.MAX_VALUE;
        if (cmdLine.hasOption("substring")) {
          sub = Integer.parseInt(cmdLine.getOptionValue("substring"));
        }

        boolean countOnly = cmdLine.hasOption("count");

        Writable key = (Writable) reader.getKeyClass().newInstance();
        Writable value = (Writable) reader.getValueClass().newInstance();
        writer.append("Key class: ")
            .append(String.valueOf(reader.getKeyClass()))
            .append(" Value Class: ").append(String.valueOf(value.getClass()))
            .append(LINE_SEP);
        writer.flush();

        long count = 0;
        if (countOnly == false) {
          while (reader.next(key, value)) {
            writer.append("Key: ").append(String.valueOf(key));
            String str = value.toString();
            writer.append(": Value: ").append(
                str.length() > sub ? str.substring(0, sub) : str);
            writer.write(LINE_SEP);
            writer.flush();
            count++;
          }
          writer.append("Count: ").append(String.valueOf(count))
              .append(LINE_SEP);
        } else {
          while (reader.next(key, value)) {
            count++;
          }
          writer.append("Count: ").append(String.valueOf(count))
              .append(LINE_SEP);
        }
        writer.flush();

        if (cmdLine.hasOption("output")) {
          writer.close();
        }
        reader.close();
      }

    } catch (ParseException e) {
      LOG.info("Error : " + e);
      cli.printUsage();
      return;
    }
  }
}
