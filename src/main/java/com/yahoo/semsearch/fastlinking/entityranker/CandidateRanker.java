package com.yahoo.semsearch.fastlinking.entityranker;

import java.util.ArrayList;

import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;
import com.yahoo.semsearch.fastlinking.view.EntityScore;

/**
 * This class ranks candidate entities for a given query. Different subclasses have to implement the
 * {@link #rank( Entity e, CandidatesInfo info, EntityContext context, String surfaceForm, int length ) rank} method
 * @author roi
 *
 */
public abstract class CandidateRanker {

    /**
     * Ranks a set of entities for a given query segment. Internally it just selects the highest ranked entity and returns it 
     * @param infos
     * @param context
     * @param q
     * @param len
     * @param k
     * @return
     */
    public ArrayList<EntityScore> rankEntities( CandidatesInfo infos, EntityContext context, String q, int len, int k ) {
  	ArrayList<EntityScore> scores = new ArrayList<EntityScore>();
  	scores.add( getHighestRankedEntity( infos, context, q, len, k ) );  	
  	return scores;
      }
    
    /**
     * Returns the highest ranked entity, along with its score, for a given query 
     * @param infos
     * @param context
     * @param surfaceForm
     * @param length
     * @param k
     * @return
     */
    public EntityScore getHighestRankedEntity( CandidatesInfo infos, EntityContext context, String surfaceForm, int length, int k ) {
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
     * @param e
     * @param info
     * @param context
     * @param surfaceForm
     * @param length
     * @return
     */
    public abstract double rank( Entity e, CandidatesInfo info, EntityContext context, String surfaceForm, int length );

}
