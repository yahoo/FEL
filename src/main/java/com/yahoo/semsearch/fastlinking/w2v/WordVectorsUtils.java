/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.yahoo.semsearch.fastlinking.view.ClusterEntry;

/**
 * Utility class for using deep learned word vectors. Provides different algebraic manipulations of the word vectors, and utilities to run
 * K-means clustering, centroid calculation and vector to vector similarity calculations.
 *
 * @author roi blanco
 */
public class WordVectorsUtils {

    /**
     * Cluster a file with words
     * @param args command line arguments; see --help
     * @throws Exception
     */
    public static void main( String[] args ) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP( WordVectorsUtils.class.getName(), "Provides an interface for reading word vectors from a serialized word vector file", new Parameter[]{
                new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Input words to cluster" ),
                new FlaggedOption( "words", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'v', "words", "Words vectors file" ),
                new FlaggedOption( "cluster", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 'k', "cluster", "Number of clusters" ),
                new FlaggedOption( "original", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "Words in the original centroids", "File with the words corresponding to the original clusters" ),
                //new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "ouptut to serialize the data" ),
        } );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;
        WordVectorsUtils reader = new WordVectorsUtils();
        ArrayList<String> words = reader.readWordsFromFile( jsapResult.getString( "input" ) );

        //

        Object2ObjectOpenHashMap<String, ClusterEntry> clusters = null;
        if( jsapResult.getString( "original" ) != null ) {
            //  clusters = reader.clusterWithOriginalCentroids( jsapResult.getString( "original" ), map );
            UncompressedWordVectors map = UncompressedWordVectors.read( jsapResult.getString( "input" ) );
            clusters = reader.assignClosest( map, reader.readWordsFromFile( jsapResult.getString( "original" ) ) );
        } else {
            CompressedW2V unigrams = new CompressedW2V( jsapResult.getString( "words" ) );
            UncompressedWordVectors map = new UncompressedWordVectors();
            map.N = unigrams.getVectorLength();
            Object2ObjectOpenHashMap<String, float[]> vectors = new Object2ObjectOpenHashMap<String, float[]>();
            map.vectors = vectors;
            for( String w : words ) {
                float[] vv = unigrams.getVectorOf( w );
                if( vv != null ) {
                    vectors.put( w, vv );
                }
            }
            clusters = reader.cluster( jsapResult.getInt( "cluster" ), map );
        }
        reader.printClusters( clusters );
    }

    /**
     * Outputs on stdout the different clusters computed
     *
     * @param cluster word clusters
     */
    public void printClusters( Object2ObjectOpenHashMap<String, ClusterEntry> cluster ) {
        int k = -1;
        for( ClusterEntry jj : cluster.values() ) {
            if( jj.cluster > k ) k = jj.cluster;
        }
        k++;
        for( int i = 0; i < k; i++ ) {
            for( String s : cluster.keySet() ) {
                if( cluster.get( s ).cluster == i ) {
                    //if ( cluster.get( s ).score > 0.5 )
                    System.out.println( s + "\t" + i + "\t" + cluster.get( s ).score );
                }
            }
        }
    }


    /**
     * Returns the centroids for an already computed clustering assignment
     *
     * @param map uncompressed word vectors
     * @param clusters original assigment
     * @return new centroids
     */
    public HashMap<Integer, float[]> calculateCentroids( UncompressedWordVectors map, Object2ObjectOpenHashMap<String, ClusterEntry> clusters ) {
        HashMap<Integer, float[]> centroids = new HashMap<>(); //a bit space wasteful but convenient in this case
        Int2IntOpenHashMap wordsPerCluster = new Int2IntOpenHashMap();
        for( String word : clusters.keySet() ) {
            float[] vectorOf = map.getVectorOf( word );
            if( vectorOf != null ) {
                final int c = clusters.get( word ).cluster;
                float[] partial = centroids.get( c );
                if( partial == null ) partial = new float[ map.N ];
                for( int i = 0; i < map.N; i++ ) partial[ i ] += vectorOf[ i ];
                centroids.put( c, partial );
                wordsPerCluster.addTo( c, 1 );
            }
        }

        for( Integer i : centroids.keySet() ) {
            float[] c = centroids.get( i );
            int nw = wordsPerCluster.get( i );
            for( int j = 0; j < map.N; j++ ) {
                c[ j ] /= nw;
            }
            centroids.put( i, c );
        }
        return centroids;
    }

    /**
     * Performs clustering of a set of vectors when a list of original words is provided for each centroid.
     *
     * @param originalCentroidsFile file containing the original words
     * @param map uncompressed vectors
     * @return cluster assignment
     * @throws IOException
     */
    public Object2ObjectOpenHashMap<String, ClusterEntry> clusterWithOriginalCentroids( String originalCentroidsFile, UncompressedWordVectors map ) throws IOException {
        ArrayList<String> words = readWordsFromFile( originalCentroidsFile );
        String[] order = createStringArray( map );
        Object2IntOpenHashMap<String> wordMap = createWordMap( order );
        int[] originalAssign = originalAssignment( map, words, wordMap );
        return cluster( words.size(), map, originalAssign, order );
    }

    /**
     * Creates a String[] out of the keys of an @see UncompressedWordVectors
     *
     * @param map uncompressed word vectors
     * @return string array with the word keys
     */
    public String[] createStringArray( UncompressedWordVectors map ) {
        String[] mapS = new String[ map.getVectorLength() ];
        int z = 0;
        for( String s : map.vectors.keySet() ) {
            mapS[ z++ ] = s;
        }
        return mapS;
    }

    /**
     * Creates a (reversed) hash assignment from a String array, where the assignment is the index each string has in the original array
     *
     * @param x original string array
     * @return reversed hash assigment
     */
    private Object2IntOpenHashMap<String> createWordMap( String[] x ) {
        Object2IntOpenHashMap<String> map = new Object2IntOpenHashMap<String>();
        for( int i = 0; i < x.length; i++ ) {
            map.put( x[ i ], i );
        }
        return map;
    }

    /**
     * Parses an input file and returns the phrases found in each line in an ArrayList<String>
     *
     * @param input input file with words/phrases
     * @return words in the fiel
     * @throws IOException
     */
    public ArrayList<String> readWordsFromFile( String input ) throws IOException {
        final BufferedReader lines = new BufferedReader( new FileReader( input ) );
        String line;
        ArrayList<String> words = new ArrayList<String>();
        while( ( line = lines.readLine() ) != null ) {
            String nLine = line.trim();
            if( !nLine.isEmpty() ) words.add( line.trim() );
        }
        lines.close();
        return words;
    }

    public int[] originalAssignment( final UncompressedWordVectors map, ArrayList<String> words, Object2IntOpenHashMap<String> order ) {
        final int len = map.vectors.entrySet().size();
        int[] original = new int[ len ];
        Random r = new Random();
        for( int i = 0; i < len; i++ ) {
            original[ i ] = r.nextInt( words.size() );
        }
        int j = 0;
        for( String s : words ) {
            original[ order.get( s ) ] = j++;
        }
        return original;
    }

    /**
     * k-means of the words in the vector file
     * @param k number of clusters
     * @param map vector
     * @return clustering of the words in the vector
     */
    public Object2ObjectOpenHashMap<String, ClusterEntry> cluster( final int k, final UncompressedWordVectors map ) {
        final int len = map.vectors.keySet().size();
        int[] original = new int[ len ];
        Random r = new Random();
        for( int i = 0; i < k; i++ ) {
            original[ i ] = i;
        }
        for( int i = k + 1; i < len; i++ ) {
            original[ i ] = r.nextInt( k );
        }

        String[] words = new String[ map.vectors.keySet().size() ];
        int z = 0;
        for( String s : map.vectors.keySet() ) {
            words[ z++ ] = s;
        }
        return cluster( k, map, original, words );
    }

    /**
     * Returns the closest word in the vector file
     * @param map vectors
     * @param words input words
     * @return a list of closest words to the input ones
     */
    public Object2ObjectOpenHashMap<String, ClusterEntry> assignClosest( final UncompressedWordVectors map, ArrayList<String> words ) {
        final int d = map.getVectorLength();
        Object2ObjectOpenHashMap<String, ClusterEntry> assignment = new Object2ObjectOpenHashMap<String, ClusterEntry>();
        for( String w : map.vectors.keySet() ) {
            float maxSim = -1;
            int bestWord = -1;
            for( int j = 0; j < words.size(); j++ ) {
                final float s = sim( map.vectors.get( w ), map.vectors.get( words.get( j ) ), d );
                if( s > maxSim ) {
                    maxSim = s;
                    bestWord = j;
                }
            }
            ClusterEntry e = new ClusterEntry();
            e.cluster = bestWord;
            e.score = maxSim;
            assignment.put( w, e );
        }
        return assignment;
    }

    public Object2ObjectOpenHashMap<String, ClusterEntry> cluster( final int k, final UncompressedWordVectors map, int[] originalAssigment, String[] words ) {
        final int N = words.length;
        int z = 0;
        final int d = map.N;
        float[][] x = new float[ N ][];

        for( z = 0; z < N; z++ ) {
            x[ z ] = map.vectors.get( words[ z ] );
        }
        final int maxiter = 100;
        Object2ObjectOpenHashMap<String, ClusterEntry> clusters = new Object2ObjectOpenHashMap<String, ClusterEntry>();
        //compute original centroids
        float[][] centroids = computeCentroids( x, k, d, N, originalAssigment );

        int[] assignment = new int[ N ];
        boolean done = false;
        int iterations = 0;
        while( !done ) {
            boolean change = false;
            int[] newAssignment = new int[ N ];
            for( int i = 0; i < N; i++ ) {
                float maxSim = -1;
                int bestCentroid = 0;
                for( int j = 0; j < k; j++ ) {
                    final float s = sim( centroids[ j ], x[ i ], d );
                    if( s > maxSim ) {
                        maxSim = s;
                        bestCentroid = j;
                    }
                }
                newAssignment[ i ] = bestCentroid;
                if( bestCentroid != assignment[ i ] ) change = true;
            }
            assignment = newAssignment;
            if( iterations++ > maxiter || !change ) done = true;
            //recompute the centroids

            centroids = computeCentroids( x, k, d, N, newAssignment );
        }
        //build the output
        for( int i = 0; i < N; i++ ) {
            ClusterEntry e = new ClusterEntry();
            e.cluster = assignment[ i ];
            e.score = sim( centroids[ assignment[ i ] ], x[ i ], d );
            clusters.put( words[ i ], e );
        }
        return clusters;
    }

    public float[][] computeCentroids( float[][] x, int k, int d, int N, int[] assignment ) {
        float[][] centroids = new float[ k ][ d ];
        int[] elementsInCluster = new int[ k ];
        for( int z = 0; z < N; z++ ) {
            for( int p = 0; p < d; p++ ) {
                centroids[ assignment[ z ] ][ p ] += x[ z ][ p ];
            }
            elementsInCluster[ assignment[ z ] ]++;
        }
        for( int z = 0; z < k; z++ ) {
            for( int p = 0; p < d; p++ )
                centroids[ z ][ p ] /= elementsInCluster[ z ];
        }
        return centroids;
    }

    /**
     * Computes the centroid of a list of words
     * @param words list of words
     * @param N number of dimensions
     * @param vectors word vectors
     * @return centroid of the words
     */
    public static float[] centroid( Collection<String> words, int N, WordVectors vectors ) {
        float[] acum = new float[ N ];
        int wordsWithFeatures = 0;
        for( String w : words ) {
            float[] v = vectors.getVectorOf( w );
            if( v != null ) {
                wordsWithFeatures++;
                for( int i = 0; i < N; i++ )
                    acum[ i ] += v[ i ];
            }
        }
        if( wordsWithFeatures == 0 ) return acum;
        for( int i = 0; i < N; i++ ) {
            acum[ i ] /= wordsWithFeatures;
        }
        return acum;
    }

    /**
     * dot product
     * @param v one vector
     * @param w  another vector
     * @param N dimensions
     * @return w * v
     */
    public static float sim( float[] v, float[] w, int N ) { //L2 norm	
        float score = 0;
        float la = 0;
        float lb = 0;
        for( int i = 0; i < N; i++ ) {
            score += v[ i ] * w[ i ];
            la += w[ i ] * w[ i ];
            lb += v[ i ] * v[ i ];
        }
        if( la == 0 || lb == 0 ) return 0; //avoiding NaN
        return ( float ) ( score / ( Math.sqrt( la ) * Math.sqrt( lb ) ) );
    }

    /**
     * Computes the centroid of the unigrams in a query
     * @param query original query
     * @param N dimension
     * @param vectors word vectors
     * @return centroid vector
     */
    public static float[] queryCentroid( String query, int N, WordVectors vectors ) {
        ArrayList<String> words = new ArrayList<String>();
        String[] chunks = chunk( query );
        for( String s : chunks ) {
            words.add( s );
        }
        return WordVectorsUtils.centroid( words, N, vectors );
    }


    private static String[] chunk( String query ) {
        return query.split( "\\s" );
    }

}
