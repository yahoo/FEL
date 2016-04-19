
/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
*/

package com.yahoo.semsearch.fastlinking.io;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * From https://lintool.github.io/Cloud9/
 * Tool for building the mapping between Wikipedia internal ids (docids) and sequentially-numbered
 * ints (docnos).
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikipediaDocnoMappingBuilder extends Configured implements Tool, DocnoMapping.Builder {
	private static final Logger LOG = Logger.getLogger(WikipediaDocnoMappingBuilder.class);
	private static final Random RANDOM = new Random();

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE, OTHER
	};

	private static class MyMapper extends Mapper<LongWritable, WikipediaPage, IntWritable, IntWritable> {
		private final static IntWritable keyOut = new IntWritable();
		private final static IntWritable valOut = new IntWritable(1);

		private boolean keepAll;

		@Override
		public void setup(Context context) {
			keepAll = context.getConfiguration().getBoolean(KEEP_ALL_OPTION, false);
		}

		@Override
		public void map(LongWritable key, WikipediaPage p, Context context) throws IOException, InterruptedException {

			// If we're keeping all pages, don't bother checking.
			if (keepAll) {

				if (p.getTitle().startsWith("Help:") || p.getTitle().startsWith("Wikipedia:") || p.getTitle().startsWith("User:"))
					return;

				context.getCounter(PageTypes.TOTAL).increment(1);
				keyOut.set(Integer.parseInt(p.getDocid()));
				context.write(keyOut, valOut);
				return;

			}

			context.getCounter(PageTypes.TOTAL).increment(1);

			if (p.isRedirect()) {
				context.getCounter(PageTypes.REDIRECT).increment(1);
			} else if (p.isEmpty()) {
				context.getCounter(PageTypes.EMPTY).increment(1);
			} else if (p.isDisambiguation()) {
				context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
			} else if (p.isArticle()) {
				// heuristic: potentially template or stub article
				if (p.getTitle().length() > 0.3 * p.getContent().length()) {
					context.getCounter(PageTypes.OTHER).increment(1);
					return;
				}

				context.getCounter(PageTypes.ARTICLE).increment(1);

				if (p.isStub()) {
					context.getCounter(PageTypes.STUB).increment(1);
				}

				keyOut.set(Integer.parseInt(p.getDocid()));
				context.write(keyOut, valOut);
			} else {
				context.getCounter(PageTypes.NON_ARTICLE).increment(1);
			}
		}
	}

	private static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private final static IntWritable cnt = new IntWritable(1);

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, cnt);
			cnt.set(cnt.get() + 1);
		}
	}

	@Override
	public int build(Path src, Path dest, Configuration conf) throws IOException {
		super.setConf(conf);
		try {
			return run(new String[] { "-" + INPUT_OPTION + "=" + src.toString(), "-" + OUTPUT_FILE_OPTION + "=" + dest.toString() });
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_FILE_OPTION = "output_file";
	public static final String KEEP_ALL_OPTION = "keep_all";
	public static final String LANGUAGE_OPTION = "wiki_language";

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("XML dump file").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output file").create(OUTPUT_FILE_OPTION));
		options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr|it").hasArg().withDescription("two-letter language code").create(LANGUAGE_OPTION));
		options.addOption(KEEP_ALL_OPTION, false, "keep all pages");

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_FILE_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String language = null;
		if (cmdline.hasOption(LANGUAGE_OPTION)) {
			language = cmdline.getOptionValue(LANGUAGE_OPTION);
			if (language.length() != 2) {
				System.err.println("Error: \"" + language + "\" unknown language!");
				return -1;
			}
		}

		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		String outputFile = cmdline.getOptionValue(OUTPUT_FILE_OPTION);
		boolean keepAll = cmdline.hasOption(KEEP_ALL_OPTION);

		String tmpPath = "tmp-" + WikipediaDocnoMappingBuilder.class.getSimpleName() + "-" + RANDOM.nextInt(10000);

		LOG.info("Tool name: " + this.getClass().getName());
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output file: " + outputFile);
		LOG.info(" - keep all pages: " + keepAll);
		LOG.info(" - language: " + language);

		Job job = Job.getInstance(getConf());
		job.setJarByClass(WikipediaDocnoMappingBuilder.class);
		job.setJobName(String.format("BuildWikipediaDocnoMapping[%s: %s, %s: %s, %s: %s]", INPUT_OPTION, inputPath, OUTPUT_FILE_OPTION, outputFile,
				LANGUAGE_OPTION, language));

		job.getConfiguration().setBoolean(KEEP_ALL_OPTION, keepAll);
		if (language != null) {
			job.getConfiguration().set("wiki.language", language);
		}
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(tmpPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already.
		FileSystem.get(getConf()).delete(new Path(tmpPath), true);

		if (job.waitForCompletion(true)) {

			//			long cnt = keepAll ? job.getCounters().findCounter(PageTypes.TOTAL).getValue() : job.getCounters().findCounter(PageTypes.ARTICLE).getValue();
			long cnt = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
			WikipediaDocnoMapping.writeDocnoMappingData(FileSystem.get(getConf()), tmpPath + "/part-r-00000", (int) cnt, outputFile);
			FileSystem.get(getConf()).delete(new Path(tmpPath), true);
			return 0;

		} else {
			return -1;
		}
	}

	public WikipediaDocnoMappingBuilder() {
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WikipediaDocnoMappingBuilder(), args);
	}
}
