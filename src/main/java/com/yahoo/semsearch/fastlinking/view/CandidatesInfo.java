/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.view;

import java.io.Serializable;


/**
 * Holds information about the different candidates that match a surface form
 * This class should expose an iterator over the different candidates
 * (minimally)
 *
 * @author roi blanco
 */
public class CandidatesInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    public final Entity[] entities;
    public EntityScore maxScore = null;
    public double QAF; //The query frequency. The number of time the query got issued on US web search and led to at least one Wikipedia document.
    public double QAT; //The query total frequency. The number of time this query got issued on US web search.
    //   public double MAF; //The matching alias (within the query) frequency. The number of time the matching alias occurred in queries issued on the US web traffic and led to at least one Wikipedia document. (WAC)
    //  public double MAT; //The matching alias (within the query) total frequency. The number of time the matching alias occurred in queries issued on US web search.
    public double LAF; //The anchor text link frequency. The number of time the alias occurred in the Wikipedia corpus as a link / anchor text pointing to another Wikipedia document. (WANC)
    public double LAT; //The anchor text total frequency. The number of time the alias occurred in the Wikipedia corpus. (WC)
    public double totalPriorProbOfClick = 0;
    public double totalPriorProbOfLink = 0; //these three are new and used for smoothing
    public double QAC;

    public CandidatesInfo( Entity[] entities, int QAF, int QAT, int QAC, int LAF, int LAT ) {
        this.entities = entities;
        this.QAF = QAF;
        this.QAT = QAT;
        this.QAC = QAC;
        //this.MAF = MAF;
        //this.MAT = MAT;
        this.LAF = LAF;
        this.LAT = LAT;
        for( Entity e : entities ) {
            totalPriorProbOfClick += e.QEF; //there's a problem here, we don't have the prior click prob for a query (we don't know how many times it was displayed)
            totalPriorProbOfLink += e.LET;
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append( "QAF:" );
        sb.append( QAF );
        sb.append( " " );
        sb.append( "QAT:" );
        sb.append( QAT );
        sb.append( " " );
//	sb.append( "MAF:" );
//	sb.append( MAF );
//	sb.append( " ");
//	sb.append( "MAT:" );
//	sb.append( MAT );
//	sb.append( " ");
        sb.append( "LAF:" );
        sb.append( " " );
        sb.append( LAF );
        sb.append( " " );
        sb.append( "LAT:" );
        sb.append( " " );
        sb.append( LAT );
        sb.append( "\n" );
        for( Entity e : entities ) {
            sb.append( "\t" + e.toString() + "\n" );
        }
        return sb.toString();
    }

}
