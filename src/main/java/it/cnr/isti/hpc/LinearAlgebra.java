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

////from https://github.com/ot/entity2vec
public class LinearAlgebra {
    public static float inner( int size, float[] v1, int offset1, float[] v2, int offset2 ) {
        if( size % 4 != 0 ) {
            throw new IllegalArgumentException( "Vector size must be a multiple of 4" );
        }

        float x0 = 0, x1 = 0, x2 = 0, x3 = 0;
        // manually unroll to help the compiler autovectorizer
        // (current JVM does not support vectorized accumulation)
        for( int i = 0; i < size; i += 4 ) {
            x0 += v1[ offset1 + i + 0 ] * v2[ offset2 + i + 0 ];
            x1 += v1[ offset1 + i + 1 ] * v2[ offset2 + i + 1 ];
            x2 += v1[ offset1 + i + 2 ] * v2[ offset2 + i + 2 ];
            x3 += v1[ offset1 + i + 3 ] * v2[ offset2 + i + 3 ];
        }

        return x0 + x1 + x2 + x3;
    }
}
