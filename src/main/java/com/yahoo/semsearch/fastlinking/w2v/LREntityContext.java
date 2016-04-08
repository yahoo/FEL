package com.yahoo.semsearch.fastlinking.w2v;

import com.yahoo.semsearch.fastlinking.hash.AbstractEntityHash;
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import it.cnr.isti.hpc.Word2VecCompress;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;

/**
 * Computes an entity score using the similarity of two vectors:
 * - sigmoid-smoothed centroid of the context words
 * - sigmoid-smoothed centroid of the entity words
 *
 * @author roi blanco
 */
public class LREntityContext extends CentroidEntityContext {

    public LREntityContext( String wordsFile, String entityFile, AbstractEntityHash hash ) throws ClassNotFoundException, IOException {
        Word2VecCompress vec = ( Word2VecCompress ) BinIO.loadObject( entityFile );
        scorer = new CustomLREntityScorer( ( Word2VecCompress ) BinIO.loadObject( wordsFile ), vec );
        this.hash = ( QuasiSuccinctEntityHash ) hash;
        init( vec );
    }


    @Override
    public String toString() {
        return "LREntityContext";
    }

    @Override
    public float queryNormalizer() {
        return ( ( CustomLREntityScorer.CustomLRScorerContext) context).queryNormalizer();
    }
}