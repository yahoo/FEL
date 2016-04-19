/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * Stores a set of HashSets with language models.
 * Each hash corresponds to a type and the values in the HashSet are (word,frequency) pairs.
 * These language models are not compressed/optimized. If the application requires or benefits from larger LMs these could be hashed differently.
 * The class provides a main method to build the language models given an input file.
 *
 * @author roi blanco
 */
public class TypeLanguageModel implements Serializable {

    private static final long serialVersionUID = 1L;
    public Object2ObjectLinkedOpenHashMap<String, Object2IntOpenHashMap<String>> languageModels;
    public Object2IntOpenHashMap<String> freqs;
    public Object2IntOpenHashMap<String> backgroundModel;
    public int totalFreq;

    /**
     * Creates the language model data structure given a file with the format
     * Type <TAB> (frequency, words), (frequency, words) ... Type <TAB>
     * (frequency, words), (frequency, words) ... ....
     *
     * @param args command line arguments
     * @throws JSAPException
     * @throws IOException
     */
    public static void main( String args[] ) throws JSAPException, IOException {
        SimpleJSAP jsap = new SimpleJSAP( TypeLanguageModel.class.getName(), "Language model for types builder", new Parameter[]{ new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Input file" ), new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "output", "File to serialize the data structure" ), } );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;

        final BufferedReader lines = new BufferedReader( new FileReader( jsapResult.getString( "input" ) ) );

        Object2ObjectLinkedOpenHashMap<String, Object2IntOpenHashMap<String>> languageModels = new Object2ObjectLinkedOpenHashMap<String, Object2IntOpenHashMap<String>>();
        Object2IntOpenHashMap<String> freqs = new Object2IntOpenHashMap<String>();
        Object2IntOpenHashMap<String> background = new Object2IntOpenHashMap<String>();
        int totalFreq = 0;
        String line = "";
        while( ( line = lines.readLine() ) != null ) {
            String[] parts = line.split( "\t" );
            Object2IntOpenHashMap<String> typeHash = languageModels.get( parts[ 0 ] );
            if( typeHash == null ) {
                typeHash = new Object2IntOpenHashMap<String>();
                languageModels.put( parts[ 0 ], typeHash );
                freqs.put( parts[ 0 ], 0 );
            }
            String[] scoreAndModifier = parts[ 1 ].split( "," );
            Integer freqI = 0;
            for( String s : scoreAndModifier ) {
                Matcher m = Pattern.compile( "(\\()(.+)" ).matcher( s ); // freq
                if( m.find() ) {
                    Double freqD = ( Double.parseDouble( m.group( 2 ) ) ); //might break if input is ill-formed
                    freqI = freqD.intValue();
                }

                m = Pattern.compile( "(.+?)(\\))" ).matcher( s );
                if( m.find() ) { //words
                    String[] tmpQ = Normalize.normalize( m.group( 1 ) ).trim().split( "\\s" );
                    //generate unigrams + bigrams + trigrams

                    for( String queryS : getTrigrams( tmpQ ) ) {
                        Integer freq = typeHash.get( queryS );
                        if( freq == null ) {
                            typeHash.put( queryS, 0 );
                            background.put( queryS, 0 );
                        }
                        freqs.addTo( parts[ 0 ], freqI );
                        typeHash.addTo( queryS, freqI );
                        background.addTo( queryS, freqI );
                        totalFreq += freqI;
                    }
                }
            }
        }
        lines.close();
        TypeLanguageModel model = new TypeLanguageModel();
        model.languageModels = languageModels;
        model.freqs = freqs;
        model.totalFreq = totalFreq;
        model.backgroundModel = background;
        BinIO.storeObject( model, jsapResult.getString( "output" ) );
    }

    /**
     * Extracts trigrams from a string array
     *
     * @param words list of words to extract trigrams from
     * @return list of trigrams
     */
    private static List<String> getTrigrams( String[] words ) {
        List<String> query = new ArrayList<String>();

        for( int i = 0; i < words.length - 2; i++ ) { //all trigrams, most bigrams +  unigrams
            query.add( words[ i ] );
            query.add( words[ i ] + "_" + words[ i + 1 ] );
            query.add( words[ i ] + "_" + words[ i + 1 ] + "_" + words[ i + 2 ] );
        }
        if( words.length > 1 ) { //last bigram
            query.add( words[ words.length - 2 ] + "_" + words[ words.length - 1 ] );
            query.add( words[ words.length - 2 ] );
        }
        if( words.length > 0 ) {//last unigram
            query.add( words[ words.length - 1 ] );
        }
        return query;
    }

    /**
     * Extracts trigrams from a String list
     *
     * @param words list of words to extract trigrams from
     * @return list of trigrams
     */
    public static List<String> getTrigrams( List<String> words ) {
        List<String> query = new ArrayList<String>();
        final int l = words.size();
        for( int i = 0; i < l - 2; i++ ) { //all trigrams, most bigrams +  unigrams
            query.add( words.get( i ) );
            query.add( words.get( i ) + "_" + words.get( i + 1 ) );
            query.add( words.get( i ) + "_" + words.get( i + 1 ) + "_" + words.get( i + 2 ) );
        }
        if( l > 1 ) { //last bigram
            query.add( words.get( l - 2 ) + "_" + words.get( l - 1 ) );
            query.add( words.get( l - 2 ) );
        }
        if( l > 0 ) {//last unigram
            query.add( words.get( l - 1 ) );
        }
        return query;
    }
}
