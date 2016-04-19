/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.hash;

import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.StringAndCandidate;

/**
 * Reads the input datapack. This datapack is computed using a hadoop pipeline
 * offline (see the io package). The datapack has one alias per line.
 *
 * @author roi blanco
 */

public class FormatReader {
    private final static Logger LOGGER = LoggerFactory.getLogger( FormatReader.class );

    /**
     * Provides an iterator over the surface form and different candidate
     * entities that might be linked to this surface form, along with their
     * features
     *
     * @param input          input file name
     * @param queryThreshold minimum number of counts on query logs to store the candidate
     * @param wikiThreshold  minimum number of counts on wiki anchor to store the candidate
     * @return
     */
    public static Iterable<StringAndCandidate> stringAndCandidates( final String input, final int queryThreshold, final int wikiThreshold ) {
        return new Iterable<StringAndCandidate>() {
            @Override
            public Iterator<StringAndCandidate> iterator() {
                try {
                    return new AbstractObjectIterator<StringAndCandidate>() {
                        LineIterator lineIterator = new LineIterator( new FastBufferedReader( new InputStreamReader( new FileInputStream( input ), Charsets.UTF_8 ) ) );

                        @Override
                        public boolean hasNext() {
                            return lineIterator.hasNext();
                        }

                        @Override
                        public StringAndCandidate next() {
                            if( !hasNext() ) throw new NoSuchElementException();
                            StringAndCandidate c;
                            while( ( c = parseLine( lineIterator.next().toString(), queryThreshold, wikiThreshold ) ) == null ) {
                                if( !lineIterator.hasNext() ) {
                                    StringAndCandidate sc = new StringAndCandidate();
                                    sc.candidatesInfo = new CandidatesInfo( new Entity[ 0 ], 0, 0, 0, 0, 0 );
                                    sc.surfaceForm = "<FOO-FOO>";
                                    return sc;
                                }
                            }
                            return c;
                        }
                    };
                } catch( FileNotFoundException e ) {
                    throw new RuntimeException( e.getMessage(), e );
                }
            }
        };

    }

    /**
     * Creates a set of candidates out of a line alias + \u0001 FEATURES where
     * FEATURES are alias QAF QAC QAT MAF MAT LAF LAT ATE and are separated with
     * \u0001
     *
     * @param line input line
     * @return parsed candidate information
     */
    public static StringAndCandidate parseLine( String line, int queryThreshold, int wikiThreshold ) {
        StringAndCandidate candidate = new StringAndCandidate();
        String[] parts = line.split( "\t+" );
        String[] kk = parts[ 0 ].split( "\u0001" );

        candidate.surfaceForm = kk[ 0 ];
        ArrayList<Entity> ents = new ArrayList<Entity>();
        //alias QAF  QAC  QAT MAF  MAT LAF LAT ATE
        int QAF = Integer.parseInt( kk[ 1 ] );
        int QAC = Integer.parseInt( kk[ 2 ] );
        int QAT = Integer.parseInt( kk[ 3 ] );
        //int MAF = Integer.parseInt( kk[ 4 ] );
        //int MAT = Integer.parseInt( kk[ 5 ] );
        int LAF = Integer.parseInt( kk[ 6 ] );
        int LAT = Integer.parseInt( kk[ 7 ] );
        //int ATE = Integer.parseInt( kk[ 8 ] );

        //yes, we're ignoring MAF MAT and ATE
        for( int i = 1; i < parts.length; i++ ) {
            String[] moreParts = parts[ i ].trim().split( "\u0001" );
            for( int j = 0; j < moreParts.length; j++ ) {
                Entity entity = new Entity();
                entity.id = Integer.parseInt( moreParts[ j++ ] );
                entity.type = ( short ) Integer.parseInt( moreParts[ j++ ] );
                entity.QEF = Integer.parseInt( moreParts[ j++ ] );
                entity.QAEF = Integer.parseInt( moreParts[ j++ ] );
                entity.MET = Integer.parseInt( moreParts[ j++ ] );
                entity.MAET = Integer.parseInt( moreParts[ j++ ] );
                entity.LET = Integer.parseInt( moreParts[ j++ ] );
                entity.LAET = Integer.parseInt( moreParts[ j++ ] );

                //if( entity.LAET > wikiThreshold || entity.QAEF > queryThreshold || moreParts.length < 2 ){ //min threshold, also keeping unique aliases
                ents.add( entity );
            }
        }

        if( ents.size() > 0 ) {
            //pruning!
            double maxQAEF = -Double.MAX_VALUE;
            Entity max = null;
            for( Entity e : ents ) { //keep the maxscore
                if( e.QAEF > maxQAEF ) {
                    max = e;
                    maxQAEF = e.QAEF;
                }
            }
            ArrayList<Entity> newE = new ArrayList<Entity>();
            newE.add( max );
            for( Entity e : ents ) { //keep the maxscore
                if( ( e.LAET >= wikiThreshold || e.QAEF >= queryThreshold ) && !e.equals( max ) ) {
                    newE.add( e );
                } else {
                    if( !e.equals( max ) )
                        LOGGER.info( "removing " + e + " laet? " + e.LAET + ">= " + wikiThreshold + "?" + ( e.LAET >= wikiThreshold ) + " qaef?" + ( e.QAEF >= queryThreshold ) + " wikT " + wikiThreshold + " qT " +
                                queryThreshold );
                }
            }
            candidate.candidatesInfo = new CandidatesInfo( ents.toArray( new Entity[ newE.size() ] ), QAF, QAT, QAC, LAF, LAT );

        } else {
            LOGGER.error( " no entities for alias " + candidate.surfaceForm );
            return null;
        }
        return candidate;
    }

    public static void main( String[] args ) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP( FormatReader.class.getName(), "Tests the reading format", new Parameter[]{ new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Input file" ), } );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;
        final Iterable<StringAndCandidate> stringAndCandidates = FormatReader.stringAndCandidates( jsapResult.getString( "input" ), 0, 0 );
        int bogus = 0;
        int numberOfCandidates = 0;
        for( StringAndCandidate sc : stringAndCandidates ) {
            CandidatesInfo ci = sc.candidatesInfo;
            if( ci.QAF == 0 && ci.LAF == 0 && ci.LAT == 0 && ci.QAC == 0 && ci.QAT == 0 ) bogus++;
            numberOfCandidates++;
            if( sc.surfaceForm.contains( "brad pitt" ) ) System.out.println( sc.surfaceForm + "\n" + ci );
        }

        LOGGER.info( "Candidates with all zeros : " + bogus );
        LOGGER.info( "Number of candidates:" + numberOfCandidates );
    }
}
