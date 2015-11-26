package com.yahoo.semsearch.fastlinking.w2v;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author roi
 *
 */

public class Quantizer {
    private static Logger log = LoggerFactory.getLogger( Quantizer.class );

    public class ErrorHolder {
	public int words;
	public double error;

	public ErrorHolder( int words, double error ) {
	    this.words = words;
	    this.error = error;
	}
    }

    public double golombBits( float[] v ) {
	double[] nat_v = new double[ v.length ];
	float abs_sum = 0;
	for ( int i = 0; i < v.length; i++ ) {
	    nat_v[ i ] += v[ i ] >= 0 ? 2 * v[ i ] : -( 2 * v[ i ] + 1 );
	    abs_sum += nat_v[ i ];
	}
	if ( abs_sum == 0 ) return 0F;
	double f = ( (float) v.length ) / abs_sum;
	double m = Math.ceil( Math.log( 2 - f ) / -Math.log( 1 - f ) );
	double b = Math.ceil( Math.log( m ) );
	double acc = 0;
	double ex = Math.pow( 2, b );
	for ( int i = 0; i < v.length; i++ ) {
	    double vv = nat_v[ i ] < ex - m ? b - 1 : b;
	    acc += nat_v[ i ] / m + 1 + vv;
	}
	return acc;
    }

    /**
     * Format: <numWords> <vector size> <quantization factor> Words (one per
     * line) Vectors (one per line)
     * 
     * @param modelFile
     * @param outputFile
     * @param q
     * @throws IOException
     */
    public void serialize( String modelFile, String outputFile, int q, int numberOfWords ) throws IOException {
	log.info( "Serializing quantized model to " + outputFile + " using q = " + q );
	BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( outputFile ), "UTF-8" ) );
	BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( modelFile ) ) ) );
	String line = br.readLine();
	line = br.readLine();//skip the header - we don't trust it
	int len = line.split( "\\s+" ).length - 1;
	bw.write( numberOfWords + "\t" + len + "\t" + q );
	bw.write( "\n" );
	br.close();

	br = new BufferedReader( new InputStreamReader( new FileInputStream( modelFile ), "UTF-8" ) );
	br.readLine(); //skip the header
	while ( ( line = br.readLine() ) != null ) {
	    String[] parts = line.split( "\\s+" );
	    bw.write( parts[ 0 ] );
	    bw.write( "\n" );
	}
	br.close();

	br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( modelFile ) ) ) );
	br.readLine(); //skip the header
	while ( ( line = br.readLine() ) != null ) {
	    String[] parts = line.split( "\\s+" );
	    for ( int i = 1; i < parts.length; i++ ) {
		Double ff = new Double( parts[ i ] );
		int qa = (int) ( (int) ( Math.abs( ff ) * q ) * Math.signum( ff ) );
		bw.write( qa + " " ); //this might blow up depending on how it's parsed later on
	    }
	    bw.write( "\n" );
	}
	br.close();
	bw.close();
    }

    /**
     * To avoid storing everything in memory, we read the input file each time
     * 
     * @param modelFile
     * @param q
     * @throws IOException
     */
    public ErrorHolder quantizeArray( String modelFile, int q ) throws IOException {
	final BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( modelFile ) ) ) );
	String line = null;
	double error = 0;
	int items = 0;
	while ( ( line = br.readLine() ) != null ) {
	    String[] parts = line.split( "\\s+" );
	    double norm = 0;
	    items++;
	    double wordError = 0;
	    double v[] = new double[ parts.length ];
	    for ( int i = 1; i < parts.length; i++ ) {
		v[ i ] = new Double( parts[ i ] );
		norm += v[ i ] * v[ i ];
	    }
	    norm = Math.sqrt( norm );
	    for ( int i = 1; i < parts.length; i++ ) {
		int qa = (int) ( (int) ( Math.abs( v[ i ] ) * q ) * Math.signum( v[ i ] ) );
		//mean_err = np.mean(np.sqrt(np.sum((dequant_A - A)**2, axis=-1)) / A_norms)
		double dqa = ( qa + 0.5 * Math.signum( qa ) ) / q;
		wordError += ( v[ i ] - dqa ) * ( v[ i ] - dqa );
		//	System.out.println( v[ i ] + "\t" + qa + "\t" + dqa + " \t error " + error );
	    }
	    error += Math.sqrt( wordError ) / norm;
	}
	br.close();
	return new ErrorHolder( items - 1, error / items );
    }

    public static void main( String args[] ) throws IOException {
	Quantizer q = new Quantizer();
	q.quantize( args[ 0 ], args[ 1 ], 0.1 );
    }

    /**
     * You have two losses - number of bits used to compress - reconstruction
     * error (after quantizing) The standard mode is to specify a target
     * reconstruction error and stop there. The other option is to select a
     * tradeoff between the two losses and optimize
     * 
     * @param inputFile
     * @param outputFile
     * @throws IOException
     */
    public void quantize( String inputFile, String outputFile, double targetError ) throws IOException {
	int low = 1;
	int high = 128;
	int bestQ = 0;
	int nWords = 0;

	/*
	 * double bestError = Double.MAX_VALUE; for( int c = low; c < high; c *=
	 * 2 ){ ErrorHolder err = quantizeArray( inputFile, c ); nWords =
	 * err.words; log.info("Binary search: q=" + c + " err= " + err.error );
	 * if( err.error < bestError ){ bestError = err.error; bestQ = c; } }
	 */
	//if you want rice coding you could start from 1 to 128 and increment by *2 each time (and stop when the condition is met)
	
	while ( high - low > 1 ) {
	    bestQ = ( high + low ) / 2;
	    ErrorHolder err = quantizeArray( inputFile, bestQ );
	    nWords = err.words;
	    log.info( "Binary search: q=" + bestQ + " err= " + err.error );
	    if ( err.error > targetError ) {
		low = bestQ;
	    } else {
		high = bestQ;
	    }
	}

	serialize( inputFile, outputFile, bestQ, nWords );
    }
}