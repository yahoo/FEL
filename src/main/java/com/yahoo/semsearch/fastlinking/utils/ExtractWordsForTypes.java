/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.yahoo.semsearch.fastlinking.EntityContextFastEntityLinker;
import com.yahoo.semsearch.fastlinking.hash.FormatReader;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.StringAndCandidate;

/**
 * Extract word lists for different types found in a datapack and stores them into an output file
 *
 * @author roi blanco
 */
public class ExtractWordsForTypes {
    private final static Logger LOGGER = LoggerFactory.getLogger( ExtractWordsForTypes.class );

    /**
     *
     * @param args command line arguments (see -help)
     * @throws JSAPException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void main( String args[] ) throws JSAPException, ClassNotFoundException, IOException {
        SimpleJSAP jsap = new SimpleJSAP( ExtractWordsForTypes.class.getName(), "Creates a file with the most frequent words per-type", new Parameter[]{
                new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Input file" ),
                new FlaggedOption( "hash", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'h', "hash", "Hash file" ),
                new FlaggedOption( "map", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'm', "map", "Entity 2 type mapping " ),
                new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "Out file" ), }
        );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;

        int numberOfPhrases;

        ProgressLogger pl = new ProgressLogger( LOGGER );

        QuasiSuccinctEntityHash hash = ( QuasiSuccinctEntityHash ) BinIO.loadObject( jsapResult.getString( "hash" ) );
        numberOfPhrases = ( int ) hash.stats.phrases;


        Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>> wordMap = new Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>>();
        Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>> wordMapB = new Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>>();
        Iterable<StringAndCandidate> stringAndCandidates2 = FormatReader.stringAndCandidates( jsapResult.getString( "input" ), 0, 0 );
        HashMap<String, String> id2Type = EntityContextFastEntityLinker.readEntity2IdFile( jsapResult.getString( "map" ) );

        Int2ObjectOpenHashMap<String> entityTypes = new Int2ObjectOpenHashMap<String>();

        for( String typeName : id2Type.values() ) {
            wordMap.put( typeName, new Object2IntOpenHashMap<String>() );
            wordMap.get( typeName ).defaultReturnValue( 0 );
            wordMapB.put( typeName, new Object2IntOpenHashMap<String>() );
            wordMapB.get( typeName ).defaultReturnValue( 0 );

        }

        for( int i = 0; i < hash.entityNames.size(); i++ ) {
            MutableString name = hash.entityNames.get( i );
            if( !name.isEmpty() ) {
                String newType = id2Type.get( name.toString() );
                if( newType != null ) {
                    entityTypes.put( i, newType );
                }
            }
        }

        pl.set( numberOfPhrases );
        pl.start( "Extracting words" );
        for( StringAndCandidate sc : stringAndCandidates2 ) {
            String[] words = sc.surfaceForm.split( "\\s+" );
            for( Entity e : sc.candidatesInfo.entities ) {
                String newType = entityTypes.get( e.id );
                if( newType != null ) {
                    Object2IntOpenHashMap<String> wordsForType = wordMap.get( newType );
                    Object2IntOpenHashMap<String> bigramsForType = wordMapB.get( newType );
                    wordsForType.addTo( words[ 0 ], ( int ) e.QAEF );
                    for( int i = 1; i < words.length; i++ ) {
                        String pre = words[ i - 1 ] + "_" + words[ i ];
                        wordsForType.addTo( words[ 0 ], ( int ) e.QAEF );
                        bigramsForType.addTo( pre, ( int ) e.QAEF );
                    }
                }
            }
            pl.lightUpdate();
        }
        pl.stop();

        pl.start( "Writting out" );
        FileWriter fw = new FileWriter( jsapResult.getString( "output" ) );
        for( String type : wordMap.keySet() ) {
            fw.write( type + "\t" );
            Object2IntOpenHashMap<String> wordsForType = wordMap.get( type );
            for( String word : wordsForType.keySet() ) {
                fw.write( word + "\u0001" + wordsForType.get( word ) + "\t" );
            }
            wordsForType = wordMapB.get( type );
            for( String word : wordsForType.keySet() ) {
                fw.write( word + "\u0001" + wordsForType.get( word ) + "\t" );
            }
            fw.write( "\n" );
        }
        pl.stop();
        fw.close();
    }
}
