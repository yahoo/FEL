/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.entityranker;

import com.yahoo.semsearch.fastlinking.hash.CountAndRecordStats;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;

/**
 * Chunks a query and ranks entities based on selecting the max probability
 * sequence.
 *
 * @author roi blanco
 */
public class ProbabilityRanker extends CandidateRanker {
    private final boolean DEBUG = false;
    protected final CountAndRecordStats stats;
    protected final QuasiSuccinctEntityHash hash; //this is here for debugging

    public double pQuery = 0;
    public double pAnchor = 1 - pQuery;
    private double mu_0 = 20;
    private double mu_1 = mu_0; //this is completely ad-hoc and un-tuned

    public ProbabilityRanker( QuasiSuccinctEntityHash hash ) {
        this.hash = hash;
        this.stats = hash.stats;
    }

    /**
     * Scores an entity using links and query aliases
     * <p/>
     * given the factorization s -> l -> a -> e we compute p(s) p(l|s) p(a|l)
     * p(e|a,s,l)
     * <p/>
     * p(E|A) = (1/p(a)) sum_s sum_l p(E|A,s,l) p(A|s,l) p(l|s)p(s) p(a) = =
     * sum_s sum_l p(a,s,l) = sum_s sum_l p(a|s,l)p(s,l) we don't know
     * p(E|A,s=0,l=0) or p(E|A,s=1,l=0) - so we set them to zero Note: when we
     * count, it would be good to count for every subsequence in the query (not
     * just whole queries)
     * @param e entity to score
     * @param info candidates info holding stats about the string alias
     * @param context string context
     * @param surfaceForm string where the aliases are taken from
     * @param length length of the surface form
     * @return score for the entity
     */
    @Override
    public double rank( Entity e, CandidatesInfo info, EntityContext context, String surfaceForm, int length ) {
        double p_e = ( e.QEF + 1 ) / ( stats.SQEF + stats.entities ); // or ( totalClicks + numberOfCandidates )
        double p_e_w = ( e.LET + 1 ) / ( stats.SLET + stats.entities ); // or ( totalAnchor + numberOfCandidates )

        double p_l_a_w = ( info.LAF + 1 ) / ( info.LAT + 2 );//prob of linking given Wiki
        double p_l_a = ( info.QAC + 1 ) / ( info.QAT + 2 ); //prob of linking given query
        double p_e_l_a = ( e.QAEF + mu_0 * p_e ) / ( info.QAF + mu_0 ); //otherwise ( e.QAEF + 1 )  numberOfCandidates + QAF );
        double p_e_l_a_w = ( e.LAET + mu_1 * p_e_w ) / ( info.LAF + mu_1 ); // or ( e.LAET + 1 ) /  numberOfCandidates + LAF )
        if( p_e_l_a_w > 1 ) p_e_l_a_w = 1; //TODO this might happen because there were some bogus counts in datapacks
        if( p_e_l_a > 1 ) p_e_l_a = 1;
        double priorQ = ( ( info.QAT + 1 ) / ( info.QAT + info.LAT + 2 ) );
        final double score = priorQ * ( p_l_a * p_e_l_a + ( 1 - p_l_a ) * p_e ) + ( 1 - priorQ ) * ( p_l_a_w * p_e_l_a_w + ( 1 - p_l_a_w ) * p_e_w );

        if( DEBUG ) System.out.println( "\033[1m" + hash.getEntityName( e.id ) + "\033[0m type = " + e.type + " score=" + score + " " + " #Cand=" + info.entities.length + " QAEF=" + e.QAEF + " QEF= " + e.QEF + " QAF=" +
                info.QAF + " QAT=" + info.QAT + " QAC=" + info.QAC + " LAEF= " + e.LAET + " LAF=" + info.LAF + " LAT= " + info.LAT + " " + "tClicks=" + info.totalPriorProbOfClick + " p(e|l,a)=" + p_e_l_a + " p(e|l,a;" +
                "w) =" + p_e_l_a_w + "" + " priorQ = " + priorQ + " priorA " + ( 1 - priorQ ) +
                " p(l|a) = " + p_l_a + " p(l|a;w) = " + p_l_a_w + " anchorContr " + ( p_l_a_w * ( 1 - priorQ ) * p_e_l_a_w ) + " queryContr " + ( p_l_a * priorQ * p_e_l_a ) + " alias [ " + surfaceForm + " ] " );
        return Math.log( score );
    }

    /**
     * Removes the Wikipedia weight of the scoring function
     */
    public void removeWiki() {
        pQuery = 1;
        pAnchor = 0;
    }

    /**
     * Removes the query log weight of the scoring function
     */
    public void removeQuery() {
        pQuery = 0;
        pAnchor = 1;
    }

}
