package com.yahoo.semsearch.fastlinking.w2v;

import it.cnr.isti.hpc.LREntityScorer;
import it.cnr.isti.hpc.Word2VecCompress;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;

import com.yahoo.semsearch.fastlinking.hash.AbstractEntityHash;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;

/**
 * Computes an entity score using the similarity of two vectors:
 * - sigmoid-smoothed centroid of the context words
 * - sigmoid-smoothed centroid of the entity words
 *
 * @author roi
 */
public class LREntityContext extends CentroidEntityContext {

    public LREntityContext( String unigramF, String entityF, AbstractEntityHash hash ) throws ClassNotFoundException,
            IOException {
        Word2VecCompress vec = ( Word2VecCompress ) BinIO.loadObject( entityF );
        scorer = new LREntityScorer( ( Word2VecCompress ) BinIO.loadObject( unigramF ), vec );
        this.hash = ( QuasiSuccinctEntityHash ) hash;
        init( vec );
    }

    @Override
    public String toString() {
        return "LREntityContext";
    }

}
