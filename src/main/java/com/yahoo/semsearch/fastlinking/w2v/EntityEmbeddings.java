/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import it.unimi.dsi.logging.ProgressLogger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

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

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.yahoo.semsearch.fastlinking.utils.RunFELOntheGrid;

/**
 * Learns entity embeddings using regularized logistic regression and negative
 * sampling In the paper lambda=10 and rho=20
 * <p>
 * Hadoop launcher
 * hadoop jar FEL-0.1.0-fat.jar com.yahoo.semsearch.fastlinking.w2v.EntityEmbeddings -Dmapreduce.input
 * .fileinputformat.split.minsize=100 -Dmapreduce.input.fileinputformat.split.maxsize=90000 -Dmapreduce.job.maps=3000
 * -Dmapred.child.java.opts=-XX:-UseGCOverheadLimit -Dmapreduce.map.java.opts=-Xmx3g -Dmapreduce.map.memory.mb=3072 \
 * -Dmapreduce.job.queuename=queuename \ -files vectors#vectors E2W.text entity.embeddings
 *
 * @author roi blanco
 */
public class EntityEmbeddings extends Configured implements Tool {
    private Random r;

    static enum MyCounters {
        NUM_RECORDS, ERR
    };

    public EntityEmbeddings() {
        this.r = new Random( 1234 );
    }

    /**
     * Use this method to compute entity embeddings on a single machine. See --help
     * @param args command line arguments
     * @throws JSAPException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void standalonemain( String args[] ) throws JSAPException, ClassNotFoundException, IOException {
        SimpleJSAP jsap = new SimpleJSAP( EntityEmbeddings.class.getName(), "Learns entity embeddings", new Parameter[]{
                new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Entity description files" ),
                new FlaggedOption( "vectors", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'v', "vectors", "Word2Vec file" ),
                new FlaggedOption( "rho", JSAP.INTEGER_PARSER, "-1", JSAP.NOT_REQUIRED, 'r', "rho", "rho negative sampling parameters (if it's <0 we use even sampling)" ),
                new FlaggedOption( "max", JSAP.INTEGER_PARSER, "-1", JSAP.NOT_REQUIRED, 'm', "max", "Max words per entity (<0 we use all the words)" ),
                new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "Compressed version" ), }
        );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;
        CompressedW2V vectors = new CompressedW2V( jsapResult.getString( "vectors" ) );
        ProgressLogger pl = new ProgressLogger();
        final int rho = jsapResult.getInt( "rho" );
        final int nwords = vectors.getSize();
        final int d = vectors.N;
        final int maxWords = jsapResult.getInt( "max" ) > 0? jsapResult.getInt( "max" ):Integer.MAX_VALUE;
        final BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( jsapResult.getString( "input" ) ) ) ) );
        int count = 0;
        pl.count = count;
        pl.itemsName = "entities";
        while( br.readLine() != null ) count++;
        br.close();
        final PrintWriter pw = new PrintWriter( new BufferedWriter( new FileWriter( jsapResult.getString( "output" ), false ) ) );
        pw.println( count + " " + d );

        float alpha = 10;
        EntityEmbeddings eb = new EntityEmbeddings();
        final BufferedReader br2 = new BufferedReader( new InputStreamReader( new FileInputStream( new File( jsapResult.getString( "input" ) ) ) , "UTF-8") );
        pl.start();
        String line;
        while( ( line = br2.readLine() ) != null ) {
            String[] parts = line.split( "\t" );
            if( parts.length > 1 ) {
                TrainingExamples ex = eb.getVectors( parts[ 1 ], vectors, rho, nwords, maxWords );
                float[] w = eb.trainLR2( ex.x, d, ex.y, alpha );
                pw.print( parts[ 0 ] + " " );
                for( int i = 0; i < d; i++ ) {
                    pw.print( w[ i ] + " " );
                }
                pw.println();
                pl.lightUpdate();
                pw.flush();

                for( int i = 0; i < ex.y.length; i++ ) {
                    if( ex.y[ i ] > 0 ) {
                        double v = eb.scoreLR( ex.x[ i ], w );
                    }
                }
            }
        }
        br2.close();
        pw.close();
        pl.stop();
    }

    /**
     * Holder for ({x},y) sets
     */
    public class TrainingExamples {
        public TrainingExamples( float[][] x, int[] y ) {
            this.x = x;
            this.y = y;
        }
        public float[][] x;
        public int[] y;
    }

    /**
     * Gets a set of training examples out of a chunk of text using its word embeddings as features and negative sampling
     * @param input text to extract features from
     * @param vectors word embeddings
     * @param rho number of words to sample negatively
     * @param nwords total words in the vocabulary
     * @return training examples
     */
    public TrainingExamples getVectors( String input, CompressedW2V vectors, int rho, int nwords, int maxWordsPerEntity ) {
        String[] parts = input.split( "\\s+" );
        ArrayList<float[]> positive = new ArrayList<float[]>();
        ArrayList<float[]> negative = new ArrayList<float[]>();

        HashSet<Long> positiveWords = new HashSet<Long>();
        int tmp = 0;
        for( String s : parts ) {
            float[] vectorOf = vectors.getVectorOf( s );
            if( vectorOf != null ) {
                positive.add( vectorOf );
                positiveWords.add( vectors.getWordId( s ) );
                tmp++;
            }
            if( tmp > maxWordsPerEntity ) break;
        }

        int total = 0;
        if( rho < 0 ) rho = positive.size();
        while( total < rho ) {
            long xx = r.nextInt( nwords );
            while( positiveWords.contains( xx ) ) xx = r.nextInt( nwords );
            negative.add( vectors.get( xx ) );
            total++;
        }

        float[][] x = new float[ positive.size() + negative.size() ][ vectors.N ];
        int[] y = new int[ positive.size() + negative.size() ];

        for( int i = 0; i < positive.size(); i++ ) {
            x[ i ] = positive.get( i );
            y[ i ] = 1;
        }
        final int j = positive.size();
        for( int i = 0; i < negative.size(); i++ ) {
            x[ i + j ] = negative.get( i );
            y[ i + j ] = 0;
        }
        return new TrainingExamples( x, y );
    }

    /**
     * Initializes randomly the weights
     * @param N number of dimensions
     * @return a vector of N dimensions with random weights
     */
    public float[] initWeights( int N ) {
        float[] w = new float[ N ];
        for( int i = 0; i < N; i++ ) {
            w[ i ] = r.nextFloat();
        }
        return w;
    }

    /**
     * Sigmoid score
     * @param w weights
     * @param x input
     * @return 1/(1+e^( - w * x ) )
     */
    public double scoreLR( float[] w, float[] x ) {
        float inner = 0;
        for( int i = 0; i < w.length; i++ ) {
            inner += w[ i ] * x[ i ];
        }
        return 1d / ( 1 + Math.exp( -inner ) );
    }

    /**
     * Learns the weights of a L2 regularized LR algorithm
     * @param x input data
     * @param d number of dimensions
     * @param y labels
     * @param C loss-regularizer tradeoff parameter
     * @return learned weights
     */
    public float[] trainLR2( float[][] x, int d, int[] y, float C ) { //m examples. dim = N
        C = C / 2;
        final int maxIter = 50000;
        double alpha = 1D;
        final int N = y.length;
        final double tolerance = 0.00001;
        float[] w = initWeights( d );
        double preLik = 100;
        boolean convergence = false;
        int iter = 0;
        while( !convergence ) {
            double likelihood = 0;
            double[] currentScores = new double[ N ];
            float acumBias = 0;
            for( int i = 0; i < N; i++ ) {
                currentScores[ i ] = scoreLR( w, x[ i ] ) - y[ i ];
                acumBias += currentScores[ i ] * x[ i ][ 0 ];
            }
            w[ 0 ] = ( float ) ( w[ 0 ] - alpha * ( 1D / N ) * acumBias ); //bias doesn't regularize
            for( int j = 1; j < d; j++ ) {
                float acum = 0;
                for( int i = 0; i < N; i++ ) {
                    acum += currentScores[ i ] * x[ i ][ j ];
                }
                w[ j ] = ( float ) ( w[ j ] - alpha * ( ( 1D / N ) * ( acum + C * w[ j ] ) ) );

            }

            double norm = 0;
            for( int j = 0; j < d; j++ ) {
                norm += w[ j ] * w[ j ];
            }
            norm = ( C / N ) * norm;
            for( int i = 0; i < N; i++ ) {
                double nS = scoreLR( w, x[ i ] );
                if( nS > 0 ) {
                    double s = y[ i ] * Math.log( nS ) + ( 1 - y[ i ] ) * Math.log( 1 - nS );
                    if( !Double.isNaN( s ) ) likelihood += s;
                }
            }
            likelihood = norm - ( 1 / N ) * likelihood;
            iter++;
            if( iter > maxIter ) convergence = true;
            else if( Math.abs( likelihood - preLik ) < tolerance ) convergence = true;
            if( likelihood > preLik ) alpha /= 2;

            preLik = likelihood;

        }
        return w;
    }

    /**
     * Mapper class
     * @param <K> tuples read
     */
    public static class EntityEMapper<K extends WritableComparable<K>> extends Mapper<K, Text, Text, Text> {
        CompressedW2V vectors;
        int d;
        int rho = -1;
        float alpha = 10;
        int maxWords = 150;
        EntityEmbeddings eb = new EntityEmbeddings();

        public void setup( Context context ) throws IOException {
            try {
                vectors = new CompressedW2V( "vectors" );
                d = vectors.N;
            } catch( ClassNotFoundException e ) {
                e.printStackTrace();
                System.exit( -1 );
            }
        }

        @Override
        public void map( K key, Text t, Context context ) throws IOException, InterruptedException {
            final int nwords = vectors.getSize();
            String[] parts = t.toString().split( "\t" );
            if( parts.length > 1 ) {
                TrainingExamples ex = eb.getVectors( parts[ 1 ], vectors, rho, nwords, maxWords );
                if( ex.y.length > 0 ) {
                    float[] w = eb.trainLR2( ex.x, d, ex.y, alpha );
                    StringBuffer sb = new StringBuffer();
                    for( int i = 0; i < d; i++ ) {
                        sb.append( w[ i ] );
                        sb.append( " " );
                    }
                    context.write( new Text( parts[ 0 ] ), new Text( sb.toString() ) );
                }
            }
        }
    }


    /**
     * Reducer class
     */
    public static class EntityEReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
            String tt = "";
            for( Text value : values ) {
                tt += value;
                break;//we only want the first one
            }
            context.write( key, new Text( tt ) );
        }

        @Override
        public void cleanup( Context context ) throws IOException, InterruptedException {

        }
    }

    @Override
    public int run( String[] args ) throws Exception {
        Configuration conf = getConf();
        Job job = new Job( conf );
        //Job job = Job.getInstance( conf );
        job.setJarByClass( EntityEmbeddings.class );
        // Process custom command-line options
        Path in = new Path( args[ 0 ] );
        Path out = new Path( args[ 1 ] );

        FileInputFormat.setInputPaths( job, in );
        FileOutputFormat.setOutputPath( job, out );

        job.setInputFormatClass( TextInputFormat.class );
        job.setOutputFormatClass( TextOutputFormat.class );

        // Specify various job-specific parameters
        job.setJobName( "Entity embeddings" );
        job.setNumReduceTasks( 1 );
        job.setJarByClass( EntityEmbeddings.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );
        job.setMapperClass( EntityEMapper.class );
        job.setReducerClass( EntityEReducer.class );
        job.setCombinerClass( EntityEReducer.class );

        job.waitForCompletion( true );

        return 0;
    }

    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run( new Configuration(), new EntityEmbeddings(), args );
        //standalonemain( args );
        System.exit( res );
    }
}
