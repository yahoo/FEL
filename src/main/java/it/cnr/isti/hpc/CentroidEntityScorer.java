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


//from https://github.com/ot/entity2vec
public class CentroidEntityScorer extends EntityScorer {

    public CentroidEntityScorer( Word2VecCompress word_model, Word2VecCompress entity_model ) {
        super( word_model, entity_model );

        if( entity_model.dimensions() != word_model.dimensions() ) {
            throw new IllegalArgumentException( "Word and entity models have incompatible vector dimensions" );
        }
    }

    public class CentroidScorerContext extends ScorerContext {
        float[] centroid_vec;
        float norm;

        public CentroidScorerContext( float[] word_vecs, int[] word_counts ) {
            super( word_vecs, word_counts );
            // compute context centroid
            int word_size = word_model.dimensions();
            int n_words = word_counts.length;
            centroid_vec = new float[ word_size ];
            for( int i = 0; i < n_words; ++i ) {
                int word_count = word_counts[ i ];
                for( int j = 0; j < word_size; ++j ) {
                    centroid_vec[ j ] += word_count * word_vecs[ i * word_size + j ];
                }
            }

            norm = LinearAlgebra.inner( centroid_vec.length, centroid_vec, 0, centroid_vec, 0 );
            norm = ( float ) Math.sqrt( norm );
        }

        @Override
        public float compute_score() {
            int word_size = centroid_vec.length;
            return LinearAlgebra.inner( word_size, entity_vec, 0, centroid_vec, 0 ) / norm;
        }
    }

    @Override
    public ScorerContext create_context( float[] word_vecs, int[] word_counts ) {
        return new CentroidScorerContext( word_vecs, word_counts );
    }

}
