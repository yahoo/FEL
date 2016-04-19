/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.hash;

import java.io.IOException;
import java.io.Serializable;

import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.Entity;

/**
 * Counts the total number of occurrences across different event spaces
 * (~features) and stores them for later use. This is useful for computing
 * probabilities later on at ranking time.
 *
 * @author roi blanco
 */
public class CountAndRecordStats implements Serializable {

    private static final long serialVersionUID = 1L;

    public long entities, phrases, entityPhrasePairs = 0;
    public long SQAF, SQAT, SQAC, SLAF, SLAT;
    public long SQEF, SQAEF, SMET, SMAET, SLET, SLAET;

    private CountAndRecordStats( long SQAF, long SQAT, long SQAC, long SLAF, long SLAT, long SQEF, long SQAEF, long SMET, long SMAET, long SLET, long SLAET, long entities, long phrases, long entityPhrasePairs ) {

        this.entities = entities;
        this.phrases = phrases;
        this.entityPhrasePairs = entityPhrasePairs;
        this.SQAF = SQAF;
        this.SQAT = SQAT;
        this.SQAC = SQAC;
        this.SLAF = SLAF;
        this.SLAT = SLAT;
        this.SQEF = SQEF;
        this.SQAEF = SQAEF;
        this.SMET = SMET;
        this.SMAET = SMAET;
        this.SLET = SLET;
        this.SLAET = SLAET;
    }

    public static CountAndRecordStats createStats( QuasiSuccinctEntityHash quasiHash ) throws IOException {
        long SQAF = 0, SQAT = 0, SQAC = 0, SLAF = 0, SLAT = 0;
        long SQEF = 0, SQAEF = 0, SMET = 0, SMAET = 0, SLET = 0, SLAET = 0;
        long entityPhrasePairs = 0;

        for( int i = 0; i < quasiHash.hash.size(); i++ ) {
            CandidatesInfo entry = new CandidatesInfo( new Entity[ 0 ], 0, 0, 0, 0, 0 );
            try {
                entry = quasiHash.candidatesInfo( i );
            } catch( Exception e ) {
                e.printStackTrace();
                System.out.println( " Error at index " + i + " " );
                System.exit( -1 );
            }
            SQAF += entry.QAF;
            SQAT += entry.QAT;
            SQAC += entry.QAC;
            SLAF += entry.LAF;
            SLAT += entry.LAT;

            for( Entity e : entry.entities ) {
                SLET += e.LET;
                SLAET += e.LAET;
                SQAEF += e.QAEF;
                SQEF += e.QEF;
                entityPhrasePairs++;
            }
        }

        for( int i = 0; i < quasiHash.entityNames.size(); i++ ) {
            Entity e = quasiHash.getEntity( i );
            SQEF += e.QEF;
            SMET += e.MET;

        }
        return new CountAndRecordStats( SQAF, SQAT, SQAC, SLAF, SLAT, SQEF, SQAEF, SMET, SMAET, SLET, SLAET, quasiHash.entityNames.size(), quasiHash.hash.size(), entityPhrasePairs );
    }

}
