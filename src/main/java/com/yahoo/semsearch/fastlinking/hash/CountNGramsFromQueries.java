/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.hash;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.MWHCFunction;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.StringAndCandidate;

/**
 * This is a helper class to rewrite the datapack. The reason you might want to
 * do this is because the current datapack might only count full strings (queries, anchor) as events, but
 * the model operates on a term by term basis so some of the inner n-grams might
 * be under represented in the counts. Essentially this class just counts the
 * ngrams over all the aliases and updates the counts of ever (inner) alias
 * found.
 *
 * @author roi blanco
 */
public class CountNGramsFromQueries {
    private final static Logger LOGGER = LoggerFactory.getLogger( CountNGramsFromQueries.class );


    public static void main( String args[] ) throws JSAPException, ClassNotFoundException, IOException {
        SimpleJSAP jsap = new SimpleJSAP( QuasiSuccinctEntityHash.class.getName(), "Creates a MPHF from a file with the candidates info",
                new Parameter[]{
                        new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Input file" ),
                        new FlaggedOption( "hash", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'h', "hash", "Hash file" ),
                        new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "Compressed version" ), }
        );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;

        int numberOfPhrases;
        Object2LongFunction<? extends CharSequence> f;
        ProgressLogger pl = new ProgressLogger( LOGGER );
        if( jsapResult.getString( "hash" ) != null ) {
            QuasiSuccinctEntityHash hash = ( QuasiSuccinctEntityHash ) BinIO.loadObject( jsapResult.getString( "hash" ) );
            numberOfPhrases = ( int ) hash.stats.phrases;
            f = hash.hash;
        } else {
            final Iterable<StringAndCandidate> stringAndCandidates = FormatReader.stringAndCandidates( jsapResult.getString( "input" ), 0, 0 );

            Iterable<CharSequence> surfaceForms = new Iterable<CharSequence>() {
                @Override
                public Iterator<CharSequence> iterator() {
                    return new AbstractObjectIterator<CharSequence>() {
                        Iterator<StringAndCandidate> i = stringAndCandidates.iterator();

                        @Override
                        public boolean hasNext() {
                            return i.hasNext();
                        }

                        @Override
                        public String next() {
                            return i.next().surfaceForm;
                        }
                    };
                }
            };

            ShiftAddXorSignedStringMap surfaceForm2Position = new ShiftAddXorSignedStringMap( surfaceForms.iterator(),
                    new MWHCFunction.Builder<CharSequence>().keys( surfaceForms ).transform( TransformationStrategies.utf16() )
                            .build() );
            f = surfaceForm2Position;
            numberOfPhrases = surfaceForm2Position.size();
        }
        Iterable<StringAndCandidate> stringAndCandidates2 = FormatReader.stringAndCandidates( jsapResult.getString( "input" ), 0, 0 );


        long[] counts = new long[ numberOfPhrases ];
        pl.set( numberOfPhrases );
        pl.start( "Counting n-grams" );
        for( StringAndCandidate sc : stringAndCandidates2 ) {
            String[] words = sc.surfaceForm.split( "\\s+" );
            for( int i = 0; i < words.length; i++ ) {
                StringBuilder sb = new StringBuilder();
                for( int j = i; j < words.length; j++ ) {
                    sb.append( words[ j ] );
                    String ss = sb.toString();
                    Long l = f.get( ss );
                    if( l != null ) {
                        counts[ l.intValue() ] += sc.candidatesInfo.QAT;
                    }
                    sb.append( " " );
                }
            }
            pl.lightUpdate();
        }
        pl.stop();
        pl.start( "Flushing new datapack into " + jsapResult.getString( "output" ) );
        stringAndCandidates2 = FormatReader.stringAndCandidates( jsapResult.getString( "input" ), 0, 0 );
        Writer fw = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( jsapResult.getString( "output" ) ), "UTF-8" ) );
        for( StringAndCandidate sc : stringAndCandidates2 ) {
            fw.write( sc.surfaceForm + "\u0001" + toInt( sc.candidatesInfo.QAF ) + "\u0001" + toInt( sc.candidatesInfo.QAC ) + "\u0001" + counts[ ( ( Long ) f.get( sc.surfaceForm ) ).intValue() ] + "\u0001" + 1 +
					"\u0001" + 1 + "\u0001" + toInt( sc.candidatesInfo.LAF ) + "\u0001" + toInt( sc.candidatesInfo.LAT ) + "\u0001" + sc.candidatesInfo.entities.length );
            for( Entity e : sc.candidatesInfo.entities ) {
                fw.write( "\t" + e.id + "\u0001" + e.type + "\u0001" + toInt( e.QEF ) + "\u0001" + toInt( e.QAEF ) + "\u0001" + "1" + "\u0001" + "1" + "\u0001" + toInt( e.LET ) + "\u0001" + toInt( e.LAET ) );
            }

            fw.write( "\n" );
            pl.lightUpdate();
        }
        pl.stop();
        fw.close();
    }

    public static int toInt( double d ) {
        return new Double( d ).intValue();
    }

    //write out
    /*
     * The line format:
     * 
     * alias_info + \t + entity_info + [\t + entity_info]*
     * 
     * The alias_info format:
     * 
     * alias + \u0001 + QAF + \u0001 + QAC + \u0001 + QAT + \u0001 + MAF +
     * \u0001 + MAT + \u0001 + LAF + \u0001 + LAT + \u0001 + ATE
     * 
     * The entity_info format:
     * 
     * id + \u0001 + type_id + \u0001 + QEF + \u0001 + QAEF + \u0001 +
     * MEF + \u0001 + MAEF + \u0001 + LEF + \u0001 + LAEF
     */
}
