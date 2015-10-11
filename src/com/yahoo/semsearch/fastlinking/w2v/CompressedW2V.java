package com.yahoo.semsearch.fastlinking.w2v;

import it.cnr.isti.hpc.Word2VecCompress;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import com.yahoo.semsearch.fastlinking.w2v.WordVectors;
import com.yahoo.semsearch.fastlinking.w2v.WordVectorsUtils;

/**
 * This class is a wrapper forwarding everything to Word2VecCompress.
 * It has a main method that can be used to check similarities between word sets from the command line.
 * @author roi
 * 
 */
public class CompressedW2V implements WordVectors {

    private Word2VecCompress vec;
    public int N;

    public CompressedW2V( String fileName ) throws ClassNotFoundException, IOException {
	this.vec = (Word2VecCompress) BinIO.loadObject( fileName );
	N = vec.getInt( 1 ).length;
    }

    @Override
    public float[] getVectorOf( String word ) {
	return vec.get( word );
    }

    @Override
    public int getVectorLength() {
	return N;
    }

    
    
    public static void main( String args[] ) throws IOException, ClassNotFoundException {
	//CentroidEntityScorer scorer = new CentroidEntityScorer( (Word2VecCompress) BinIO.loadObject( args[0] ), (Word2VecCompress) BinIO.loadObject( args[1] ) );
	//LREntityScorer scorer = new LREntityScorer( (Word2VecCompress) BinIO.loadObject( args[0] ), (Word2VecCompress) BinIO.loadObject( args[1] ) );
	final BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );
	CompressedW2V vectors = new CompressedW2V( args[0] );
	String q;
	for ( ;; ) {
	    System.out.print( ">" );
	    q = br.readLine();
	    if ( q == null ) {
		System.err.println();
		break; // CTRL-D
	    }
	    if ( q.length() == 0 ) continue;
	    String[] strings = q.split("/");	    	   
	    
	    float[] q1 = WordVectorsUtils.centroid( Arrays.asList(strings[ 0 ].split(" ")), vectors.N, vectors  );
	    float[] q2 = WordVectorsUtils.centroid( Arrays.asList(strings[ 1 ].split(" ")), vectors.N, vectors  );
	    float sim = WordVectorsUtils.sim( q1, q2, vectors.N );
	    System.out.println(" sim ( [ " + strings[0] + "] , [" + strings[ 1 ] + "] ) = " + sim);
	}
    }
}
