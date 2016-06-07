
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;

/**
 * Tool for extracting anchor text out of Wikipedia. https://github.com/lintool/Cloud9/
 * @author Jimmy Lin

 */
public class ExtractWikipediaAnchorText extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(ExtractWikipediaAnchorText.class);

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
	};

	private static class MyMapper0 extends MapReduceBase implements Mapper<IntWritable, WikipediaPage, Text, Text> {

		private static final Text KEY = new Text();
		private static final Text VALUE = new Text();

		private static final Pattern redirectPattern = Pattern.compile("(#redirect)[:\\s]*(?:\\[\\[(.*?)\\]\\]|(.*))", Pattern.CASE_INSENSITIVE);

		@Override
		public void map(IntWritable key, WikipediaPage p, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			reporter.incrCounter(PageTypes.TOTAL, 1);
			KEY.set(p.getTitle());

			if (p.isRedirect()) {
				reporter.incrCounter(PageTypes.REDIRECT, 1);

				Matcher m = redirectPattern.matcher(p.getWikiMarkup());
				String redirectTarget = "";
				if (m.find()) {
					if (m.group(2) != null)
						redirectTarget = m.group(2);
					else
						redirectTarget = m.group(3);
				}

				redirectTarget = redirectTarget.trim();

				int loc = redirectTarget.indexOf('#');
				if (loc != -1)
					redirectTarget = redirectTarget.substring(0, loc);

				if (redirectTarget.length() > 0) {

					redirectTarget = capitalizeFirstChar(redirectTarget);

					// we do not want any circular references
					if (p.getTitle().trim().equals(redirectTarget))
						return;

					VALUE.set(StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(redirectTarget)).trim());
					output.collect(KEY, VALUE);
				}

			} else if (p.isDisambiguation()) {
				reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
			} else if (p.isEmpty()) {
				reporter.incrCounter(PageTypes.EMPTY, 1);
			} else if (p.isArticle()) {
				reporter.incrCounter(PageTypes.ARTICLE, 1);

				if (p.isStub()) {
					reporter.incrCounter(PageTypes.STUB, 1);
				}
			} else {
				reporter.incrCounter(PageTypes.NON_ARTICLE, 1);
			}

		}
	}

	private static class MyMapper1 extends MapReduceBase implements Mapper<IntWritable, WikipediaPage, PairOfStringInt, PairOfStrings> {

		private static final PairOfStringInt KEYPAIR = new PairOfStringInt();
		private static final PairOfStrings VALUEPAIR = new PairOfStrings();

		// Basic algorithm:
		// Emit: key = (link target article name, 0), value = (link target docid, "");
		// Emit: key = (link target article name, 1), value = (src docid, anchor text)
		@Override
		public void map(IntWritable key, WikipediaPage p, OutputCollector<PairOfStringInt, PairOfStrings> output, Reporter reporter) throws IOException {

			// This is a caveat and a potential gotcha: Wikipedia article titles are not case sensitive on
			// the initial character, so a link to "commodity" will go to the article titled "Commodity"
			// without any issue.
			String title = capitalizeFirstChar(p.getTitle());

			KEYPAIR.set(title, 0);
			VALUEPAIR.set(title, "");
			output.collect(KEYPAIR, VALUEPAIR);

			for (Link link : p.extractLinks()) {

				String anchor = link.getAnchorText();

				anchor = PunctuationDiacriticsFolder.normalize(StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(anchor)));
				if (anchor.trim().length() < 2)
					continue;

				String target = link.getTarget();

				KEYPAIR.set(capitalizeFirstChar(StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(target))).trim(), 1);
				VALUEPAIR.set(title, anchor);
				output.collect(KEYPAIR, VALUEPAIR);

			}
		}
	}

	private static class MyReducer1 extends MapReduceBase implements Reducer<PairOfStringInt, PairOfStrings, Text, PairOfStrings> {
		private static final Text SRCID = new Text();
		private static final PairOfStrings TARGET_ANCHOR_PAIR = new PairOfStrings();

		private String targetTitle;

		//		private String targetDocid;

		@Override
		public void reduce(PairOfStringInt key, Iterator<PairOfStrings> values, OutputCollector<Text, PairOfStrings> output, Reporter reporter)
				throws IOException {

			if (key.getRightElement() == 0) {
				targetTitle = key.getLeftElement();
				//				targetDocid = values.next().getLeftElement();
			} else {
				if (!key.getLeftElement().equals(targetTitle)) {
					return;
				}

				while (values.hasNext()) {
					PairOfStrings pair = values.next();
					SRCID.set(pair.getLeftElement());
					TARGET_ANCHOR_PAIR.set(targetTitle, pair.getRightElement());

					output.collect(SRCID, TARGET_ANCHOR_PAIR);
				}
			}
		}
	}

	private static class MyPartitioner1 implements Partitioner<PairOfStringInt, PairOfStrings> {
		@Override
		public void configure(JobConf job) {
		}

		@Override
		public int getPartition(PairOfStringInt key, PairOfStrings value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	private static class MyMapper2 extends MapReduceBase implements Mapper<Text, PairOfStrings, Text, Text> {

		private static enum Resolve {
			REDIRECT
		};

		private static final Text KEY = new Text();
		private static final Text VALUE = new Text();

		private THashMap<String, String> redirects = new THashMap<String, String>();

		@Override
		public void configure(JobConf job) {

			super.configure(job);

			try {

				@SuppressWarnings("deprecation")
				SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(job), new Path("redirs.dat"), job);

				Text source = new Text();
				Text target = new Text();
				while (reader.next(source, target)) {
					redirects.put(source.toString(), target.toString());
				}
				reader.close();

			} catch (IOException e) {
				e.printStackTrace();
				redirects.clear();
				return;
			}

		}

		@Override
		public void map(Text key, PairOfStrings t, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			if (redirects.size() == 0)
				throw new IOException("zero redirects");

			String entity = t.getLeftElement();
			entity = capitalizeFirstChar(entity);

			reporter.setStatus(entity);

			ArrayList<String> seen = new ArrayList<String>();
			// transitivity
			while (redirects.contains(entity)) {

				reporter.incrCounter(Resolve.REDIRECT, 1);

				String target = redirects.get(entity);

				// break loops
				if (seen.contains(target)) {
					break;
				} else {
					seen.add(target);
				}

				if (target.equals(entity))
					break;

				entity = target;

			}

			// Here we "lose" the key, i.e., from which article the link originated.
			KEY.set(entity);
			VALUE.set(t.getRightElement());

			output.collect(KEY, VALUE);
		}
	}

	private static class MyReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, HMapSIW> {
		private static final HMapSIW map = new HMapSIW();

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, HMapSIW> output, Reporter reporter) throws IOException {
			map.clear();

			Text cur;
			while (values.hasNext()) {
				cur = values.next();

				map.increment(cur.toString());
			}

			output.collect(key, map);
		}
	}

	private static class MyMapper3 extends MapReduceBase implements Mapper<IntWritable, WikipediaPage, Text, IntWritable> {

		private static final Text KEY = new Text();
		private static final IntWritable VALUE = new IntWritable(1);

		private THashSet<String> labelVocabulary = new THashSet<String>();
		private int maxLabelLength = 15;

		@Override
		public void configure(JobConf job) {

			super.configure(job);

			try {

				@SuppressWarnings("deprecation")
				SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(job), new Path("map.dat"), job);


				HMapSIW val = new HMapSIW();
				while (reader.next(new Text(), val)) {
					for (String anchor : val.keySet()) {
						labelVocabulary.add(anchor);
					}
				}

				reader.close();

				System.err.println("labelVocabulary " + labelVocabulary.size());

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		// Basic algorithm:
		// Emit: key = (anchor), value = (1) if article contains this anchor, (0) otherwise;
		@Override
		public void map(IntWritable key, WikipediaPage p, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			if (labelVocabulary.size() == 0)
				throw new IOException("zero labels to check");

			reporter.incrCounter(PageTypes.TOTAL, 1);

			if (p.isRedirect()) {
				reporter.incrCounter(PageTypes.REDIRECT, 1);
			} else if (p.isDisambiguation()) {
				reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
			} else if (p.isEmpty()) {
				reporter.incrCounter(PageTypes.EMPTY, 1);
			} else if (p.isArticle()) {
				reporter.incrCounter(PageTypes.ARTICLE, 1);
				if (p.isStub()) {
					reporter.incrCounter(PageTypes.STUB, 1);
				}
			} else {
				reporter.incrCounter(PageTypes.NON_ARTICLE, 1);
			}

			for (Link link : p.extractLinks()) {

				String anchor = PunctuationDiacriticsFolder.normalize(StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(link.getAnchorText())));

				if (anchor.trim().length() > 1 && labelVocabulary.contains(anchor)) {
					KEY.set(anchor);
					output.collect(KEY, VALUE);
				}
			}

			String content = p.getWikiMarkup();
			if (content == null)
				return;
			content = p.getContent();
			if (content == null)
				return;

			content = PunctuationDiacriticsFolder.normalize(StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(content)));
			Pattern pat = Pattern.compile(" ");
			Matcher mat = pat.matcher(content);

			Vector<Integer> matchIndexes = new Vector<Integer>();
			while (mat.find())
				matchIndexes.add(mat.start());

			for (int i = 0; i < matchIndexes.size(); i++) {

				int startIndex = matchIndexes.elementAt(i) + 1;

				if (Character.isWhitespace(content.charAt(startIndex)))
					continue;

				for (int j = Math.min(i + maxLabelLength, matchIndexes.size() - 1); j > i; j--) {

					int currIndex = matchIndexes.elementAt(j);
					String ngram = content.substring(startIndex, currIndex);

					if (labelVocabulary.contains(ngram)) {

						KEY.set(ngram);
						output.collect(KEY, VALUE);

						// ExLabel label = labels.get(ngram);
						//
						// if (label == null) {
						// label = new ExLabel(0, 0, 1, 1, new TreeMap<Integer, ExSenseForLabel>());
						// } else {
						// label.setTextOccCount(label.getTextOccCount() + 1);
						// }
						//
						// labels.put(ngram, label);

					}
				}
			}

			// now emit all of the labels we have gathered
			// for (Map.Entry<String, ExLabel> entry : labels.entrySet()) {
			// output.collect(new Text(entry.getKey()), entry.getValue());
			// }

		}
	}

	private static class MyReducer3 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		private static final IntWritable SUM = new IntWritable();

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int s = 0;
			while (values.hasNext()) {
				s += values.next().get();
			}
			SUM.set(s);
			output.collect(key, SUM);

		}
	}

	private static class MyMapper4 extends MapReduceBase implements Mapper<Text, HMapSIW, Text, HMapSIW> {

		private static final Text KEY = new Text();

		@Override
		public void map(Text entity, HMapSIW p, OutputCollector<Text, HMapSIW> output, Reporter reporter) throws IOException {

			for (String anchor : p.keySet()) {
				KEY.set(anchor);
				HMapSIW VALUE = new HMapSIW();
				VALUE.put(entity.toString(), p.get(anchor));
				output.collect(KEY, VALUE);

			}
		}
	}

	private static class MyReducer4 extends MapReduceBase implements Reducer<Text, HMapSIW, Text, HMapSIW> {

		private static final HMapSIW map = new HMapSIW();

		@Override
		public void reduce(Text anchor, Iterator<HMapSIW> values, OutputCollector<Text, HMapSIW> output, Reporter reporter) throws IOException {

			map.clear();

			while (values.hasNext()) {
				map.putAll(values.next());
			}

			output.collect(anchor, map);

		}

	}


	public boolean getMergeInHdfs(String src, String dest, JobConf conf) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(src);
		Path dstPath = new Path(dest);

		// Check if the path already exists
		if (!(fs.exists(srcPath))) {
			LOG.info("Path " + src + " does not exists!");
			return false;
		}

		if (!(fs.exists(dstPath))) {
			LOG.info("Path " + dest + " does not exists!");
			return false;
		}
		return FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf, null);
	}

	private static final String INPUT_OPTION = "input";
	private static final String ENTITYMAP_OPTION = "emap";
	private static final String ANCHORMAP_OPTION = "amap";
	private static final String CFMAP_OPTION = "cfmap";
	private static final String REDIRECTS_OPTION = "redir";
	private static final String PHASE_OPTION = "phase";

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output for entity map").create(ENTITYMAP_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output for anchor map").create(ANCHORMAP_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output for anchor cf map").create(CFMAP_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output for redirects").create(REDIRECTS_OPTION));

		options.addOption(OptionBuilder.withArgName("phase").hasArg().withDescription("set for phase two").create(PHASE_OPTION));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(ENTITYMAP_OPTION) || !cmdline.hasOption(REDIRECTS_OPTION)
				|| !cmdline.hasOption(ANCHORMAP_OPTION) || !cmdline.hasOption(CFMAP_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Random random = new Random();
		String tmp = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000);
		String phase = cmdline.getOptionValue(PHASE_OPTION);
		Boolean phaseTwo = ( phase != null && phase.equalsIgnoreCase( "2" ) );
		Boolean phaseThree = ( phase != null && phase.equalsIgnoreCase( "3" ) );
		if( !phaseTwo && !phaseThree ) {
			task0( cmdline.getOptionValue( INPUT_OPTION ), cmdline.getOptionValue( REDIRECTS_OPTION ) );
			task1( cmdline.getOptionValue( INPUT_OPTION ), tmp );
			task2( tmp, cmdline.getOptionValue( ENTITYMAP_OPTION ), cmdline.getOptionValue( REDIRECTS_OPTION ) );
		}else if ( phaseTwo ) {
			task3( cmdline.getOptionValue( INPUT_OPTION ), cmdline.getOptionValue( ENTITYMAP_OPTION ), cmdline.getOptionValue( CFMAP_OPTION ) );
		}else{
			task4( cmdline.getOptionValue( ENTITYMAP_OPTION ), cmdline.getOptionValue( ANCHORMAP_OPTION ) );
			merge( cmdline.getOptionValue( ANCHORMAP_OPTION ), cmdline.getOptionValue( CFMAP_OPTION ) );
		}

		return 0;
	}

	/**
	 * Extracts redirects and the target for each.
	 *
	 * @param inputPath
	 * @param outputPath
	 * @throws IOException
	 */
	private void task0(String inputPath, String outputPath) throws IOException {
		LOG.info("Extracting redirects (phase 0)...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);

		JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
		conf.setJobName(String.format("ExtractWikipediaAnchorText:phase0[input: %s, output: %s]", inputPath, outputPath));

		conf.setNumReduceTasks(1);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MyMapper0.class);
		conf.setReducerClass(IdentityReducer.class);

		JobClient.runJob(conf);
	}

	/**
	 * Maps from Wikipedia article to (srcID, (targetID, anchor).
	 *
	 * @param inputPath
	 * @param outputPath
	 * @throws IOException
	 */
	private void task1(String inputPath, String outputPath) throws IOException {
		LOG.info("Extracting anchor text (phase 1)...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);

		JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
		conf.setJobName(String.format("ExtractWikipediaAnchorText:phase1[input: %s, output: %s]", inputPath, outputPath));

		// 10 reducers is reasonable.
		conf.setNumReduceTasks(10);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(PairOfStringInt.class);
		conf.setMapOutputValueClass(PairOfStrings.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PairOfStrings.class);

		conf.setMapperClass(MyMapper1.class);
		conf.setReducerClass(MyReducer1.class);
		conf.setPartitionerClass(MyPartitioner1.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}

	/**
	 *
	 * Maps from (srcID, (targetID, anchor) to (targetID, (anchor, count)).
	 *
	 * @param inputPath
	 * @param outputPath
	 * @throws IOException
	 */
	private void task2(String inputPath, String outputPath, String redirPath) throws IOException {
		LOG.info("Extracting anchor text (phase 2)...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);
		Random r = new Random(  );
		//String tmpOutput = "tmp-" + this.getClass().getCanonicalName() + "-" + r.nextInt(10000);
		//LOG.info( "intermediate folder for merge " + tmpOutput );

		JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
		conf.setJobName(String.format("ExtractWikipediaAnchorText:phase2[input: %s, output: %s]", inputPath, outputPath));

		// Gathers everything together for convenience; feasible for Wikipedia.
		conf.setNumReduceTasks(1);

		try {
			DistributedCache.addCacheFile(new URI(redirPath + "/part-00000" + "#" + "redirs.dat"), conf);
			DistributedCache.createSymlink(conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		//FileOutputFormat.setOutputPath(conf, new Path(tmpOutput));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(MapFileOutputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(HMapSIW.class);

		conf.setMapperClass(MyMapper2.class);
		conf.setReducerClass(MyReducer2.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		// Clean up intermediate data.
		FileSystem.get(conf).delete(new Path(inputPath), true);

		/*
		//merge
		String finalO = outputPath+"/part-00000/data";
		FileSystem.get(conf).mkdirs( new Path( outputPath + "part-00000") );
		getMergeInHdfs( tmpOutput, finalO, conf );
		FileSystem.get(conf).delete(new Path(tmpOutput), true);
		*/
	}

	/**
	 * Extracts CF for each found anchor.
	 *
	 * @param inputPath
	 * @param mapPath
	 * @param outputPath
	 * @throws IOException
	 */
	private void task3(String inputPath, String mapPath, String outputPath) throws IOException {
		LOG.info("Extracting anchor text (phase 3)...");
		LOG.info(" - input:   " + inputPath);
		LOG.info(" - output:  " + outputPath);
		LOG.info(" - mapping: " + mapPath);

		JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
		conf.setJobName(String.format("ExtractWikipediaAnchorText:phase3[input: %s, output: %s]", inputPath, outputPath));

		conf.setNumReduceTasks(1);
		String location = "map.dat";

		try {
			DistributedCache.addCacheFile(new URI(mapPath + "/part-00000/data" + "#" + location), conf);
			//DistributedCache.addCacheFile(new URI(mapPath + "/singleentitymap.data" + "#" + location), conf);
			DistributedCache.createSymlink(conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(MapFileOutputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper3.class);
		conf.setCombinerClass(MyReducer3.class);
		conf.setReducerClass(MyReducer3.class);

		JobClient.runJob(conf);
	}

	/**
	 * Maps from (targetID, (anchor, count)) to (anchor, (targetID, count)).
	 *
	 * @param inputPath
	 * @param outputPath
	 * @throws IOException
	 */
	private void task4(String inputPath, String outputPath) throws IOException {
		LOG.info("Extracting anchor text (phase 4)...");
		LOG.info(" - input:   " + inputPath);
		LOG.info(" - output:  " + outputPath);

		JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
		conf.setJobName(String.format("ExtractWikipediaAnchorText:phase4[input: %s, output: %s]", inputPath, outputPath));

		conf.setNumReduceTasks(1);

		//FileInputFormat.addInputPath(conf, new Path(inputPath + "/part-00000/data"));
		FileInputFormat.addInputPath(conf, new Path(inputPath + "/part-*/data"));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(MapFileOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(HMapSIW.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(HMapSIW.class);

		conf.setMapperClass(MyMapper4.class);
		conf.setReducerClass(MyReducer4.class);

		JobClient.runJob(conf);
	}

	private void merge(String anchorMapPath, String dfMapPath) throws IOException {
		LOG.info("Extracting anchor text (merge)...");
		LOG.info(" - input:   " + anchorMapPath);
		LOG.info(" - output:  " + dfMapPath);

		JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
		FileSystem fs = FileSystem.get(conf);

		// Loop over anchors
		MapFile.Reader anchorMapReader = new MapFile.Reader(new Path(anchorMapPath + "/part-00000"), conf);
		MapFile.Reader dfMapReader = new MapFile.Reader(new Path(dfMapPath + "/part-00000"), conf);

		// IntWritable key = new IntWritable(Integer.parseInt(cmdline.getArgs()[0]));
		// System.out.println(key.toString());

		Text key = new Text();
		IntWritable df = new IntWritable();
		while (dfMapReader.next(key, df)) {

			//if (!key.toString().equalsIgnoreCase("Jim Durham"))
			//	continue;

			HMapSIW map = new HMapSIW();
			anchorMapReader.get(key, map);

			System.out.println(key + "\t" + df + "\t" + map.toString());

			// for (String entity : map.keySet()) {
			// System.out.println("\t" + entity + "\t" + map.get(entity) + "\n");
			// }

			break;

		}
		anchorMapReader.close();
		dfMapReader.close();
		fs.close();

	}

	public static String capitalizeFirstChar(String title) {
		String fc = title.substring(0, 1);
		if (fc.matches("[a-z]")) {
			title = title.replaceFirst(fc, fc.toUpperCase());
		}
		return title;
	}

	public ExtractWikipediaAnchorText() {
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ExtractWikipediaAnchorText(), args);
		System.exit(res);
	}
}
