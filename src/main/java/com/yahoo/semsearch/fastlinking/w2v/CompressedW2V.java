/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import it.cnr.isti.hpc.LREntityScorer;
import it.cnr.isti.hpc.Word2VecCompress;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * This class is a wrapper forwarding everything to Word2VecCompress. It also provides a command line tool for computing word similarity
 * It has a main method that can be used to check similarities between word sets from the command line.
 *
 * @author roi blanco
 */
public class CompressedW2V implements WordVectors, Serializable {

    private static final long serialVersionUID = 1L;
    private Word2VecCompress vec;
    public int N;

    public CompressedW2V( String fileName ) throws ClassNotFoundException, IOException {
        this.vec = ( Word2VecCompress ) BinIO.loadObject( fileName );
        N = vec.getInt( 1 ).length;
    }

    public CompressedW2V( ObjectInputStream ois ) throws IOException, ClassNotFoundException {
        final Object result = ois.readObject();
        ois.close();
        this.vec = ( Word2VecCompress ) result;
        this.N = vec.getInt( 1 ).length;
    }

    /**
     * Returns the vector for the word with the given id
     * @param word id of the word
     * @return word vector or null if not found
     */
    public float[] get( long word ) {
        return vec.get( word );
    }

    /**
     * @return number of dimensions of the vectors
     */
    public int getSize() {
        return vec.size();
    }

    @Override
    public float[] getVectorOf( String word ) {
        return vec.get( word );
    }

    /**
     * Gets the id of a word string
     * @param word input word
     * @return long id of word or null if it doesn't exist in the vocabulary
     */
    public Long getWordId( String word ) {
        return vec.word_id( word );
    }

    @Override
    public int getVectorLength() {
        return N;
    }


    /**
     * Command line tool to compute word and entity similarities
     * @param args arguments [entityVectors] [wordVectors]
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void main( String args[] ) throws IOException, ClassNotFoundException {
        //CentroidEntityScorer scorer = new CentroidEntityScorer( (Word2VecCompress) BinIO.loadObject( args[0] ), (Word2VecCompress) BinIO.loadObject( args[1] ) );
        Word2VecCompress wv = ( Word2VecCompress ) BinIO.loadObject( args[ 1 ] );
        LREntityScorer scorer = new LREntityScorer(  wv, ( Word2VecCompress ) BinIO.loadObject( args[ 0 ] ));


        final BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );
        CompressedW2V vectors = new CompressedW2V( args[ 0 ] );
        String q;
        for( ; ; ) {
            System.out.print( ">" );
            q = br.readLine();
            if( q == null ) {
                System.err.println();
                break; // CTRL-D
            }
            if( q.length() == 0 ) continue;
            String[] strings = q.split( "/" );
            List<String> w = Arrays.asList( strings[ 0 ].split( " " ) );
            EntityEmbeddings eb = new EntityEmbeddings();
            float sim = scorer.score( strings[ 1 ], w );
            float[] entity = wv.get( strings[ 1 ] );
            if( entity != null ) {
                for( String aW : w ) {
                    float[] v = vectors.getVectorOf( aW );
                    if( v != null ) {
                        double score = eb.scoreLR( entity, v );
                        System.out.println( "score LR " + score );
                    }
                    System.out.println();
                }

                System.out.println( " sim ( [ " + strings[ 0 ] + "] , [" + strings[ 1 ] + "] ) = " + sim );
            } else {
                System.out.println( "entity " + strings[ 1 ] + " not found" );
            }
        }
    }
}
