package com.yahoo.semsearch.fastlinking.w2v;

import it.cnr.isti.hpc.CentroidEntityScorer;
import it.cnr.isti.hpc.EntityScorer;
import it.cnr.isti.hpc.EntityScorer.ScorerContext;
import it.cnr.isti.hpc.Word2VecCompress;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;
import java.util.ArrayList;

import com.yahoo.semsearch.fastlinking.hash.AbstractEntityHash;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;

/**
 * Computes an entity score using the similarity of two vectors:
 * - centroid of the context words
 * - centroid of the entity words
 *
 * @author roi
 */
public class CentroidEntityContext extends EntityContext {
    protected EntityScorer scorer;
    protected QuasiSuccinctEntityHash hash; //TODO remove

    private ScorerContext context;

    //hack for speeding up the id look-ups
    private ArrayList<Long> idMapping;

    public CentroidEntityContext() {};

    public CentroidEntityContext( String vector, String entities, AbstractEntityHash hash ) throws
            ClassNotFoundException, IOException {
        Word2VecCompress vec = ( Word2VecCompress ) BinIO.loadObject( entities );
        scorer = new CentroidEntityScorer( ( Word2VecCompress ) BinIO.loadObject( vector ), vec );
        this.hash = ( QuasiSuccinctEntityHash ) hash; //TODO --- this is here for debugging purposes only
        init( vec );
    }

    /**
     * Creates an initial identifier look-up array
     *
     * @param vec
     */
    void init( Word2VecCompress vec ) {
        idMapping = new ArrayList<Long>( hash.entityNames.size() + 1 );
        for( int i = 0; i < this.hash.entityNames.size(); i++ ) { //
            String name = hash.getEntityName( i ).toString();
            Long x = vec.word_id( name );
            if( x != null ) {
                idMapping.add( ( long ) x.longValue() );
            } else {
                idMapping.add( 0L );
            }
        }
    }

    @Override
    public float queryNormalizer() {
        float c = context.queryNormalizer();
        return c;
        //return c > 0? c : 1;
    }

    @Override
    public void setContextWords( ArrayList<String> words ) {
        super.setContextWords( words );
        context = scorer.context( this.words );
    }

    @Override
    public double getEntityContextScore( Entity e ) {
        //TODO change here for dynamic pruning
        return context.score( idMapping.get( e.id ) );
    }

    @Override
    public String toString() {
        return "CentroidCtx";
    }

    @Override
    public void setEntitiesForScoring( Entity[] entities ) { //empty		
    }

}
