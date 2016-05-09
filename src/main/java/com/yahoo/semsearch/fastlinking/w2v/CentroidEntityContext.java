/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;


import it.cnr.isti.hpc.CentroidEntityScorer;
import it.cnr.isti.hpc.EntityScorer;
import it.cnr.isti.hpc.Word2VecCompress;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;
import java.util.ArrayList;

import com.yahoo.semsearch.fastlinking.hash.AbstractEntityHash;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.EntityContext;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;

/**
 * Computes an entity score using the similarity of two vectors:
 * - centroid of the context words
 * - centroid of the entity words
 *
 * @author roi blanco
 */
public class CentroidEntityContext extends EntityContext {
    protected EntityScorer scorer;
    protected QuasiSuccinctEntityHash hash; //TODO debug
    protected Object2FloatOpenHashMap<Long> cache;
    protected EntityScorer.ScorerContext context;

    //hack for speeding up the id look-ups
    private ArrayList<Long> idMapping;

    public CentroidEntityContext() {}

    public CentroidEntityContext( String vector, AbstractEntityHash hash ) throws ClassNotFoundException, IOException {
        Word2VecCompress vec = ( Word2VecCompress ) BinIO.loadObject( vector );
        scorer = new CentroidEntityScorer( vec, vec );
        this.hash = ( QuasiSuccinctEntityHash ) hash;
        init( vec );
    }

    public CentroidEntityContext( String vector, String entities, AbstractEntityHash hash ) throws ClassNotFoundException, IOException {
        Word2VecCompress vec = ( Word2VecCompress ) BinIO.loadObject( entities );
        scorer = new CentroidEntityScorer( ( Word2VecCompress ) BinIO.loadObject( vector ), vec );
        this.hash = ( QuasiSuccinctEntityHash ) hash;
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
                idMapping.add( ( long ) x );
            } else {
                idMapping.add( 0L );
            }
        }
    }

    @Override
    public float queryNormalizer() {
        //float c = context.queryNormalizer();
        //return c;
        //return c > 0? c : 1;
        return 1F;
    }

    @Override
    public void setContextWords( ArrayList<String> words ) {
        super.setContextWords( words );
        context = scorer.context( this.words );
        cache = new Object2FloatOpenHashMap<>(  );
    }

    public double getEntityContextScoreNoCache( Entity e ) {
        return context.score( idMapping.get( e.id ) );
    }
    @Override
    public double getEntityContextScore( Entity e ) {
        Long id =idMapping.get( e.id ); //TODO time this, it might be the case that adding to the hash is slower than recomputing for a few cache hits
        Float s = cache.get( id );
        if( s != null ) return s;
        float f =  context.score( id );
        cache.put( id, f );
        return f;
    }

    @Override
    public String toString() {
        return "CentroidCtx";
    }

    @Override
    public void setEntitiesForScoring( Entity[] entities ) {
    }

}
