/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * Class for storing in memory word vectors that are represented with floats. Use only if you have a few vectors or a lot of RAM to spare.
 *
 * @author roi blanco
 */
public class UncompressedWordVectors implements Serializable, WordVectors {

    private static final long serialVersionUID = 1L;
    public Object2ObjectOpenHashMap<String, float[]> vectors;
    public int N;


    @Override
    public int getVectorLength() {
        return N;
    }

    @Override
    public float[] getVectorOf( String word ) {
        return vectors.get( word );
    }


    public static UncompressedWordVectors read( String file ) throws IOException {
        Object2ObjectOpenHashMap<String, float[]> map = new Object2ObjectOpenHashMap<String, float[]>();
        final BufferedReader lines = new BufferedReader( new FileReader( file ) );
        String line;
        while( ( line = lines.readLine() ) != null ) {
            String parts[] = line.split( "\t" );
            float[] values = new float[ parts.length - 1 ];
            for( int i = 1; i < parts.length; i++ ) {
                values[ i - 1 ] = Float.parseFloat( parts[ i ] );
            }
            map.put( parts[ 0 ], values );
        }
        lines.close();
        UncompressedWordVectors vector = new UncompressedWordVectors();
        vector.vectors = map;
        return vector;
    }


    public static void main( String args[] ) throws JSAPException, IOException {
        SimpleJSAP jsap = new SimpleJSAP( UncompressedWordVectors.class.getName(), "Creates a Word Vector representation from a string file", new Parameter[]{ new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP
                .NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Vector file" ), new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "Output file name" ) } );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;
        UncompressedWordVectors vec = UncompressedWordVectors.read( jsapResult.getString( "input" ) );
        vec.N = vec.vectors.get( vec.vectors.keySet().iterator().next() ).length;
        BinIO.storeObject( vec, jsapResult.getString( "output" ) );
    }

}
