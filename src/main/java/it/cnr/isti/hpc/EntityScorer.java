/**
 * Copyright 2015 Giuseppe Ottaviano <giuott@gmail.com>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package it.cnr.isti.hpc;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

////from https://github.com/ot/entity2vec
public abstract class EntityScorer {

    protected Word2VecCompress word_model;
    protected Word2VecCompress entity_model;
    public static float DEFAULT_SCORE = -Float.MAX_VALUE;

    public EntityScorer( Word2VecCompress word_model, Word2VecCompress entity_model ) {
        this.word_model = word_model;
        this.entity_model = entity_model;
    }

    public abstract class ScorerContext {
        protected float[] word_vecs;
        protected int[] word_counts;
        protected float[] entity_vec;

        public ScorerContext( float[] word_vecs, int[] word_counts ) {
            this.word_vecs = word_vecs;
            this.word_counts = word_counts;
            this.entity_vec = new float[ entity_model.dimensions() ];
        }

        public float score( Long entity_id ) {
            if( entity_id == null || word_counts.length == 0 ) {
                return DEFAULT_SCORE;
            }
            entity_model.get( entity_id, entity_vec, 0 );
            return compute_score();
        }

        public float score( String entity ) {
            return score( entity_model.word_id( entity ) );
        }

        public abstract float compute_score();
    }

    public abstract ScorerContext create_context( float[] word_vecs, int[] word_counts );

    public ScorerContext context( List<String> words ) {
        Multiset<String> counter = TreeMultiset.create();
        counter.addAll( words );

        int word_dim = word_model.dimensions();
        float[] word_vecs = new float[ counter.size() * word_dim ];
        IntArrayList word_counts = new IntArrayList();
        int n_words = 0;

        for( Multiset.Entry<String> entry : counter.entrySet() ) {
            if( word_model.get( entry.getElement(), word_vecs, n_words * word_dim ) ) {
                word_counts.add( entry.getCount() );
                n_words += 1;
            }
        }
        word_counts.trim();

        return create_context( word_vecs, word_counts.elements() );
    }

    public float[] score_ids( List<Long> entities, List<String> words ) {
        float[] scores = new float[ entities.size() ];
        ScorerContext ctx = context( words );
        for( int i = 0; i < entities.size(); ++i ) {
            scores[ i ] = ctx.score( entities.get( i ) );
        }
        return scores;
    }


    public float[] score( List<String> entities, List<String> words ) {
        List<Long> entity_ids = new ArrayList<>();
        for( String entity : entities )
            entity_ids.add( entity_model.word_id( entity ) );
        return score_ids( entity_ids, words );
    }

    public float score( String entity, List<String> words ) {
        return score( Arrays.asList( entity ), words )[ 0 ];
    }
}
