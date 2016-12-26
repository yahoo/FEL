/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/
package com.yahoo.semsearch.fastlinking.entityranker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;
import com.yahoo.semsearch.fastlinking.view.EntityScore;

/**
 * This class ranks candidate entities for a given query. Different subclasses
 * have to implement the
 * {@link #rank(Entity e, CandidatesInfo info, EntityContext context, String surfaceForm, int length )
 * rank} method
 * 
 * @author roi blanco
 *
 */
public abstract class CandidateRanker {

    /**
     * Ranks a set of entities for a given query segment. Internally it just
     * selects the highest ranked entity and returns it
     * 
     * @param infos information about the current candidates for the surface form
     * @param context context to use to rank the entity
     * @param length length of the surface form
     * @param surfaceForm string from which we are selecting the candidate entities
     * @param k number of top candidates to return
     * @return k highest scored entities
     */
    public ArrayList<EntityScore> getTopKEntities( CandidatesInfo infos, EntityContext context, String surfaceForm, int length, int k ) {
        ArrayList<EntityScore> scoresTop = new ArrayList<EntityScore>();
        final int l = infos.entities.length;    
        final double[] scores = new double[ l ];
        Integer[] idx = new Integer[ l ];
        if ( k > l ) k = l;

        for ( int i = 0; i < l; i++ ) {
            idx[ i ] = i;
            scores[ i ] = rank( infos.entities[ i ], infos, context, surfaceForm, length );
        }

        Arrays.sort( idx, new Comparator<Integer>() {
            public int compare( Integer i1, Integer i2 ) {
                return -Double.compare( scores[ i1 ], scores[ i2 ] );
            }
        } );

        for ( int i = 0; i < k; i++ ) {
            scoresTop.add( new EntityScore( infos.entities[ idx[ i ] ], scores[ idx[ i ] ] ) );
        }
        return scoresTop;
    }

    /**
     * Returns the highest ranked entity, along with its score, for a given
     * query
     *
     * @param infos information about the current candidates for the surface form
     * @param context context to use to rank the entity
     * @param length length of the surface form
     * @param surfaceForm string from which we are selecting the candidate entities
     * @return top scoring entity for the surface form
     */
    public EntityScore getHighestRankedEntity( CandidatesInfo infos, EntityContext context, String surfaceForm, int length ) {
        if ( infos.maxScore != null ) return infos.maxScore;
        int index = 0;
        double maxS = -Double.MAX_VALUE;
        context.setEntitiesForScoring( infos.entities );
        for ( int i = 0; i < infos.entities.length; i++ ) {
            double s = rank( infos.entities[ i ], infos, context, surfaceForm, length );
            if ( s > maxS ) {
                maxS = s;
                index = i;
            }
        }
        infos.maxScore = new EntityScore( infos.entities[ index ], maxS );
        return infos.maxScore;
    }

    /**
     * Scores one entity with respect to a surface form (query segment)
     * 
     * @param e entity to be ranked
     * @param info information about the current candidates for the surface form
     * @param context context to use to rank the entity
     * @param length length of the surface form
     * @param surfaceForm string from which we are selecting the candidate entities
     * @return score of e for the given context and surface form
     */
    public abstract double rank( Entity e, CandidatesInfo info, EntityContext context, String surfaceForm, int length );

}
