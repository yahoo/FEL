/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.BufferedReader;
import java.io.FileReader;

import com.yahoo.semsearch.fastlinking.FastEntityLinker;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.EmptyContext;

/**
 * Utility for measuring entity linking speed
 *
 * @author roi blanco
 */
public class MeasureSpeed {
    /**
     * Use with [hash] [queries]
     *
     * @param args input files
     * @throws Exception
     */
    public static void main( String args[] ) throws Exception {
        double threshold = 0.001;
        QuasiSuccinctEntityHash hash = ( QuasiSuccinctEntityHash ) BinIO.loadObject( args[ 0 ] );
        BufferedReader lines = new BufferedReader( new FileReader( args[ 1 ] ) );
        FastEntityLinker fel = new FastEntityLinker( hash, new EmptyContext() );
        String q;
        int numberOfQueries = 0;
        long acumTime = 0;
        while( ( q = lines.readLine() ) != null ) {
            numberOfQueries++;
            if( q.length() == 0 ) continue;
            long time = -System.currentTimeMillis();
            try {
                fel.getResults( q, threshold );
            } catch( Exception e ) {

            }
            time += System.currentTimeMillis();
            acumTime += time;
            if( numberOfQueries % 10000 == 0 ) System.out.println( " #q = " + numberOfQueries + " " + ( ( double ) acumTime / numberOfQueries ) + " ms/q, time=" + acumTime );
        }
        lines.close();
        System.out.println( " #q = " + numberOfQueries + " " + ( ( double ) acumTime / numberOfQueries ) + " ms/q, time=" + acumTime );
    }
}
