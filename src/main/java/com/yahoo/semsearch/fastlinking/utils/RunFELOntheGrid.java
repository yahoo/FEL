/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yahoo.semsearch.fastlinking.EntityContextFastEntityLinker;
import com.yahoo.semsearch.fastlinking.FastEntityLinker;
import com.yahoo.semsearch.fastlinking.FastEntityLinker.EntityResult;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.EmptyContext;
import com.yahoo.semsearch.fastlinking.view.EntitySpan;

/**
 * Runs entity linking on hadoop. It requires some files (-files) to be shipped to every node.
 * <p>
 * hadoop jar FEL-0.1-jar-with-dependencies.jar -Dmapred.map.tasks=100 -Dmapreduce.map.java.opts=-Xmx3g -Dmapreduce.map.memory.mb=3072 -Dmapred.job.queue.name=adhoc \
 * -files hash.qsi#hash,wordsvector#words,entity_vectors#entities  queriesInHDFS <outputfile>
 *
 * @author roi blanco
 */
public class RunFELOntheGrid extends Configured implements Tool {
    final private static double threshold = -6;
    static final String[] ID_SW = new String[]{ "the", "of", "a", "at", "in" };

    static enum MyCounters {
        NUM_RECORDS, ERR
    }

    ;

    public static class FELMapper<K extends WritableComparable<K>> extends Mapper<K, Text, Text, LongWritable> {
        protected QuasiSuccinctEntityHash hash;
        protected FastEntityLinker fel;
        protected HashMap<Short, String> entity2Id; //new

        public void setup( Context context ) throws IOException {
            try {
                hash = ( QuasiSuccinctEntityHash ) BinIO.loadObject( "hash" );
                fel = new FastEntityLinker( hash, new EmptyContext() );
                entity2Id = EntityContextFastEntityLinker.readTypeMapping( "mapping" );

            } catch( ClassNotFoundException e ) {
                e.printStackTrace();
                System.exit( -1 );
            }
        }

        @Override
        public void map( K key, Text t, Context context ) throws IOException, InterruptedException {
            String[] parts = t.toString().split( "\t" );
            String q = parts[ parts.length - 1 ];
            q = Normalize.normalize( q ); //we're doing this twice
            q = q.replaceAll( "\\+", " " ).toLowerCase();
            List<EntityResult> results = fel.getResults( q, threshold );
            if( results.size() > 0 ) {
                EntityResult res = results.get( 0 );
                String typeofEntity = entity2Id.get( ( ( EntitySpan ) res.s ).e.type );
                if( typeofEntity == null ) typeofEntity = "NF";
                String intentPart = Normalize.getIntentPart( q, res.text.toString() );
                String resultString = typeofEntity + "\t" + q + "\t" + intentPart + " \t " + res.text;
                //Pig-friendly custom output
        /*StringBuffer sb = new StringBuffer();
		sb.append( "(" );
		sb.append( q );
		sb.append( ",{" );
		for( EntityResult er : results ){
		    //Entity entity = hash.getEntity( er.id );
		    String typeName = typeMapping.get( er.text.toString().trim() );
		    if( typeName == null ) typeName = "NF";
		    sb.append("(");
		    sb.append( er.s.span );
		    sb.append(",");
		    sb.append( er.text );
		    sb.append(",");
		    sb.append( er.id );
		    sb.append( ",");
		    sb.append( typeName );
		    //sb.append( entity.type );
		    sb.append("," );
		    sb.append( er.score );
		    sb.append(")");
		}
		sb.append( "})" );				
		String resultString = sb.toString();
		*/
                context.getCounter( MyCounters.NUM_RECORDS ).increment( 1 );
                context.write( new Text( resultString ), new LongWritable( 1 ) );
            }
        }
    }

    public static class FELReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce( Text key, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException {
            long valueSum = 0;
            for( LongWritable value : values ) {
                valueSum += value.get();
                break;//we only want the first one
            }
            context.write( key, new LongWritable( valueSum ) );
        }

        @Override
        public void cleanup( Context context ) throws IOException, InterruptedException {

        }
    }

    public int run( String[] args ) throws Exception {
        Configuration conf = getConf();
        Job job = new Job( conf );
        //Job job = Job.getInstance( conf );
        job.setJarByClass( RunFELOntheGrid.class );
        // Process custom command-line options
        Path in = new Path( args[ 0 ] );
        Path out = new Path( args[ 1 ] );
        FileInputFormat.setInputPaths( job, in );
        FileOutputFormat.setOutputPath( job, out );

        job.setInputFormatClass( TextInputFormat.class );
        job.setOutputFormatClass( TextOutputFormat.class );

        // Specify various job-specific parameters
        job.setJobName( "Entity Linker" );
        job.setNumReduceTasks( 100 );
        job.setJarByClass( RunFELOntheGrid.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class );
        job.setMapperClass( FELMapper.class );
        job.setReducerClass( FELReducer.class );
        job.setCombinerClass( FELReducer.class );

        job.waitForCompletion( true );

        return 0;
    }

    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run( new Configuration(), new RunFELOntheGrid(), args );
        System.exit( res );
    }

}
