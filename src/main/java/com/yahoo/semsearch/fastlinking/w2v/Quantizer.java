/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import com.martiansoftware.jsap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Quantizes a word2vec-like vector file given a quantization factor, or it will binary search for the optimum one (given a reconstruction error target)
 * java -Xmx5G -cp FEL-0.1.0-fat.jar com.yahoo.semsearch.fastlinking.w2v.Quantizer -i vector -o quantized -d -q 9 -h
 * @author roi blanco
 */

public class Quantizer {
    private static Logger log = LoggerFactory.getLogger( Quantizer.class );

    /**
     * Inner class for holding the error and number of words
     */
    public class ErrorHolder {
        public int words;
        public double error;

        public ErrorHolder( int words, double error ) {
            this.words = words;
            this.error = error;
        }
    }

    /**
     * Computes the number of bits needed to store a vector
     * @param v vector
     * @return number of bits of the compressed representation
     */
    public double golombBits( float[] v ) {
        double[] nat_v = new double[ v.length ];
        float abs_sum = 0;
        for( int i = 0; i < v.length; i++ ) {
            nat_v[ i ] += v[ i ] >= 0 ? 2 * v[ i ] : -( 2 * v[ i ] + 1 );
            abs_sum += nat_v[ i ];
        }
        if( abs_sum == 0 ) return 0F;
        double f = ( ( float ) v.length ) / abs_sum;
        double m = Math.ceil( Math.log( 2 - f ) / -Math.log( 1 - f ) );
        double b = Math.ceil( Math.log( m ) );
        double acc = 0;
        double ex = Math.pow( 2, b );
        for( int i = 0; i < v.length; i++ ) {
            double vv = nat_v[ i ] < ex - m ? b - 1 : b;
            acc += nat_v[ i ] / m + 1 + vv;
        }
        return acc;
    }

    /**
     * Format: <numWords> <vector size> <quantization factor> Words (one per
     * line) Vectors (one per line)
     *
     * @param modelFile vector file
     * @param outputFile output file
     * @param q quantization factor
     * @throws IOException
     */
    public void serialize( String modelFile, String outputFile, int q, int numberOfWords, boolean hashheader ) throws
            IOException {
        log.info( "Serializing quantized model to " + outputFile + " using q = " + q );
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( outputFile ), "UTF-8" ) );
        BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( modelFile ) ) ) );
        String line = null;
        if( hashheader ) br.readLine();

        line = br.readLine();
        int len = line.split( "\\s+" ).length - 1;
        bw.write( numberOfWords + "\t" + len + "\t" + q );
        bw.write( "\n" );
        br.close();

        br = new BufferedReader( new InputStreamReader( new FileInputStream( modelFile ), "UTF-8" ) );
        if( hashheader) br.readLine(); //skip the header
        while( ( line = br.readLine() ) != null ) {
            String[] parts = line.split( "\\s+" );
            bw.write( parts[ 0 ] + "\n");
        }
        br.close();

        br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( modelFile ) ) ) );
        if( hashheader ) br.readLine(); //skip the header
        while( ( line = br.readLine() ) != null ) {
            String[] parts = line.split( "\\s+" );
            for( int i = 1; i < parts.length; i++ ) {
                Double ff = new Double( parts[ i ] );
                int qa = ( int ) ( ( int ) ( Math.abs( ff ) * q ) * Math.signum( ff ) );
                bw.write( qa + " " ); //this might blow up depending on how it's parsed later on
            }
            bw.write( "\n" );
        }
        br.close();
        bw.close();
    }


    /**
     * Writes out to disk the quantized vectors
     * @param modelFile input file
     * @param outputFile output file
     * @param q quantized factor
     * @param numberOfWords number of entries in the file
     * @param hasheader whether the file has a header or not
     * @throws IOException
     */
    public void serializeW2VFormat( String modelFile, String outputFile, int q, int numberOfWords, boolean hasheader ) throws
            IOException {
        log.info( "Serializing quantized model to " + outputFile + " using q = " + q );
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( outputFile ), "UTF-8" ) );
        BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( modelFile ) ) ) );
        String line = null;
        if( hasheader ) br.readLine();

        line = br.readLine();
        int len = line.split( "\\s+" ).length - 1;
        bw.write( numberOfWords + "\t" + len + "\t" + q );
        bw.write( "\n" );
        br.close();

        br = new BufferedReader( new InputStreamReader( new FileInputStream( modelFile ), "UTF-8" ) );
        if( hasheader) br.readLine(); //skip the header
        while( ( line = br.readLine() ) != null ) {
            String[] parts = line.split( "\\s+" );
            bw.write( parts[ 0 ] + " ");
            for( int i = 1; i < parts.length; i++ ) {
                Double ff = new Double( parts[ i ] );
                int qa = ( int ) ( ( int ) ( Math.abs( ff ) * q ) * Math.signum( ff ) );
                bw.write( qa + " " );
            }
        }
        br.close();
        bw.close();
    }

    /**
     * To avoid storing everything in memory, we read the input file each time
     *
     * @param modelFile original vector file
     * @param q quantization factor
     * @throws IOException
     * @return number of words and reconstruction error
     */
    public ErrorHolder quantizeArray( String modelFile, int q ) throws IOException {
        final BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( modelFile  ) ) ) );
        String line = null;
        double error = 0;
        int items = 0;
        while( ( line = br.readLine() ) != null ) {
            String[] parts = line.split( "\\s+" );
            double norm = 0;
            items++;
            double wordError = 0;
            double v[] = new double[ parts.length ];
            for( int i = 1; i < parts.length; i++ ) {
                v[ i ] = new Double( parts[ i ] );
                norm += v[ i ] * v[ i ];
            }
            norm = Math.sqrt( norm );
            for( int i = 1; i < parts.length; i++ ) {
                int qa = ( int ) ( ( int ) ( Math.abs( v[ i ] ) * q ) * Math.signum( v[ i ] ) );
                double dqa = ( qa + 0.5 * Math.signum( qa ) ) / q;
                wordError += ( v[ i ] - dqa ) * ( v[ i ] - dqa );
            }
            error += Math.sqrt( wordError ) / norm;
        }
        br.close();
        return new ErrorHolder( items - 1, error / items );
    }

    /**
     * Quantizes the word vectors without attempting to look for the optimal error-rate quantizer
     *
     * @param inputFile initial file to quantize
     * @param outputFile output file name
     * @param q quantization factor
     * @param hasheader whether the original file has a header or not
     * @throws IOException
     */
    public void quantizeSinglePass( String inputFile, String outputFile, int q, boolean hasheader, boolean w2vFormat ) throws IOException {
        if( !w2vFormat) serialize( inputFile, outputFile, q, countWords( inputFile, hasheader ), hasheader );
        else  serializeW2VFormat( inputFile, outputFile, q, countWords( inputFile, hasheader ), hasheader );
    }

    /**
     * @param inputfile vector file
     * @param hasHeader whether the input file has a header or not
     * @return number of words in the original vector file
     * @throws IOException
     */
    public int countWords( String inputfile, boolean hasHeader ) throws IOException {
        int items = 0;
        String line = null;
        final BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( inputfile ) ) ) );
        if( hasHeader ) br.readLine();
        while( ( line = br.readLine() ) != null ) {
            items++;
        }
        System.out.println("Found ["+items+"] words ");
        return items;
    }

    /**
     * You have two losses - number of bits used to compress - reconstruction
     * error (after quantizing) The standard mode is to specify a target
     * reconstruction error and stop there. The other option is to select a
     * tradeoff between the two losses and optimize
     *
     * @param inputFile original vector file
     * @param outputFile output file
     * @param hasheader whether the input file has a header or not
     * @throws IOException
     */
    public void quantize( String inputFile, String outputFile, double targetError, boolean hasheader, boolean w2vFormat ) throws IOException {
        int low = 1;
        int high = 128;
        int bestQ = 0;
        int nWords = 0;

        //if you want rice coding you could start from 1 to 128 and increment by *2 each time (and stop when the
        // condition is met)
        while( high - low > 1 ) {
            bestQ = ( high + low ) / 2;
            ErrorHolder err = quantizeArray( inputFile, bestQ );
            nWords = err.words;
            log.info( "Binary search: q=" + bestQ + " err= " + err.error );
            if( err.error > targetError ) {
                low = bestQ;
            } else {
                high = bestQ;
            }
        }
        if( ! w2vFormat) serialize( inputFile, outputFile, bestQ, nWords, hasheader );
        else serializeW2VFormat( inputFile,outputFile,bestQ, nWords, hasheader );
    }

    /**
     * Main class for quantizing word vectors
     * @param args command line arguments (see --help)
     * @throws IOException
     * @throws JSAPException
     */
    public static void main( String args[] ) throws IOException, JSAPException {
        SimpleJSAP jsap = new SimpleJSAP( Quantizer.class.getName(), "Learns entity embeddings", new Parameter[]{
                new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Entity description files" ),
                new Switch( "direct", 'd', "direct", "use a direct quantizer and not binary search" ),
                new Switch( "hashheader", 'h', "hashheader", "if the embeddings file has a header present (will skip it)" ),
                new FlaggedOption( "quantizer", JSAP.STRING_PARSER, "10", JSAP.NOT_REQUIRED, 'q', "quantizer", "Quantizer value" ),
                new FlaggedOption( "error", JSAP.STRING_PARSER, "0.35", JSAP.NOT_REQUIRED, 'e', "error", "Error rate" ),
                new Switch( "w2v", 'w', "w2v", "Serialize the quantized vectors using the original w2v format. If you want to compress the vectors later, you must -not- use this option" ),
                new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "Compressed version" ), }
        );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;
        Quantizer q = new Quantizer();
        if( jsapResult.getBoolean( "direct" ) ) {
            System.out.println( "Using as a quantizer " + jsapResult.getString( "quantizer" ) + " (won't attempt to search for a better one) " );
            q.quantizeSinglePass( jsapResult.getString( "input" ), jsapResult.getString( "output" ), Integer.parseInt( jsapResult.getString( "quantizer" ) ), jsapResult.getBoolean( "hashheader" ), jsapResult
                    .getBoolean( "w2v" ) );
        } else {
            q.quantize( jsapResult.getString( "input" ), jsapResult.getString( "output" ), Double.parseDouble( jsapResult.getString( "error" ) ), jsapResult.getBoolean( "hashheader" ), jsapResult.getBoolean( "w2v" ) );
        }
    }

}