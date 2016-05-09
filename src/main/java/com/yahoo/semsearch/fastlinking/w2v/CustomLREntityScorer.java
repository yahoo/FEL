/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;


import it.cnr.isti.hpc.EntityScorer;
import it.cnr.isti.hpc.LinearAlgebra;
import it.cnr.isti.hpc.Word2VecCompress;

/**
 * Wrapper for LREntityScorer - use this class with the entity embeddings learned from @see com.yahoo.semsearch.fastlinking.w2v.EntityEmbeddings
 * It computes a query (text) normalizer for score comparison
 * @author roi blanco
 */
public class CustomLREntityScorer extends EntityScorer {

    public CustomLREntityScorer( Word2VecCompress word_model, Word2VecCompress entity_model ) {
        super( word_model, entity_model );
        if( entity_model.dimensions() != word_model.dimensions() ) {
            throw new IllegalArgumentException( "Word and entity models have incompatible vector dimensions " + entity_model.dimensions() + " != " + word_model.dimensions() );
        }
    }

    public class CustomLRScorerContext extends ScorerContext {
        public CustomLRScorerContext( float[] word_vecs, int[] word_counts ) {
            super( word_vecs, word_counts );
        }

        @Override
        public float compute_score() {
            int n_words = word_counts.length;
            int word_size = word_model.dimensions();
            float s = 0;
            for( int i = 0; i < n_words; ++i ) {
                int word_count = word_counts[ i ];
                int word_offset = i * word_size;
                double dotprod = 0; //entity_vec[word_size]; //the original code added a bias
                dotprod -= LinearAlgebra.inner(word_size, word_vecs,
                		word_offset, entity_vec, 0);
                s += word_count * Math.log( 1 + Math.exp( dotprod ) );
            }
            return -s;
        }

        public float queryNormalizer() {
            int n_words = word_counts.length;
            int word_size = word_model.dimensions();
            float s = 0;
            for( int i = 0; i < n_words; ++i ) {
                int word_count = word_counts[ i ];
                int word_offset = i * word_size;
                double dotprod = 0;
                dotprod += LinearAlgebra.inner(word_size, word_vecs,
                		word_offset, word_vecs, word_offset);
                s += word_count * Math.log( 1 + Math.exp( dotprod ) );
            }
            return s != 0 ? -s : 1;
        }

    }

    @Override
    public ScorerContext create_context( float[] word_vecs, int[] word_counts ) {
        return new CustomLRScorerContext( word_vecs, word_counts );
    }


}
