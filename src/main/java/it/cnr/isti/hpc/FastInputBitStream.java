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

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.LongIterators;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.sux4j.util.EliasFanoLongBigList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

////from https://github.com/ot/entity2vec
public class FastInputBitStream {

    private ByteBuffer buf;
    private long current;
    private int fill;

    public FastInputBitStream( byte[] a ) {
        buf = ByteBuffer.wrap( a );
        buf.order( ByteOrder.BIG_ENDIAN ); // should be default anyway
        fill = 0;
        current = 0;
    }

    void refill() {
        assert fill <= 32;
        if( buf.remaining() < 4 ) {
            slow_refill();
            return;
        }
        current = ( current << 32 ) | ( buf.getInt() & 0xFFFFFFFFL );
        fill += 32;
    }

    private void slow_refill() {
        assert fill <= 32;
        assert buf.remaining() < 4;
        while( buf.hasRemaining() ) {
            current = ( current << 8 ) | ( buf.get() & 0xFFL );
            fill += 8;
        }
    }

    public void position( long pos ) {
        int wordPos = ( int ) ( pos / 32 );
        buf.position( wordPos * 4 ); // we need to preserve the alignment
        current = buf.getInt() & 0xFFFFFFFFL;
        fill = 32 - ( int ) ( pos % 32 );
    }

    public int readInt( int len ) {
        if( len > fill ) {
            refill();
        }
        assert len <= fill;
        return ( int ) ( current >>> ( fill -= len ) ) & ( ( 1 << len ) - 1 );
    }

    public int readUnary() {
        int x = 0;

        // heuristic
        if( fill < 32 ) {
            refill();
        }

        while( true ) {
            int z = Long.numberOfLeadingZeros( current << ( 64 - fill ) );
            if( z < fill ) { // This works also when fill = 0
                fill -= z + 1;
                return x + z;
            }
            x += fill;
            fill = 0;
            refill();
        }
    }

    public int readMinimalBinary( final int b ) {
        return readMinimalBinary( b, Fast.mostSignificantBit( b ) );
    }

    public int readMinimalBinary( final int b, final int log2b ) {
        if( b < 1 ) throw new IllegalArgumentException( "The bound " + b + " is not positive" );

        final int m = ( 1 << log2b + 1 ) - b;
        final int x = readInt( log2b );

        if( x < m ) return x;
        else return ( ( x << 1 ) + readInt( 1 ) - m );
    }


    public int readGolomb( final int b ) {
        return readGolomb( b, Fast.mostSignificantBit( b ) );
    }

    public int readGolomb( final int b, final int log2b ) {
        if( b < 0 ) throw new IllegalArgumentException( "The modulus " + b + " is negative" );
        if( b == 0 ) return 0;

        return readUnary() * b + readMinimalBinary( b, log2b );
    }

    static volatile long do_not_optimize;

    public static void main( String[] args ) throws IOException {
        Random rng = new Random( 1729 );
        // test readInt
        {
            int n = ( 1 << 16 ) + 1;
            int[] bs = new int[ n ];
            int[] values = new int[ n ];
            FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
            OutputBitStream obs = new OutputBitStream( oa, 0 );

            for( int i = 0; i < n; ++i ) {
                bs[ i ] = rng.nextInt( 31 ); // XXX 33?
                values[ i ] = rng.nextInt( 1 << bs[ i ] );
                obs.writeInt( values[ i ], bs[ i ] );
            }

            obs.close();
            oa.trim();

            FastInputBitStream ibs = new FastInputBitStream( oa.array );
            int pos = 0;
            for( int i = 0; i < n; ++i ) {
                long val = ibs.readInt( bs[ i ] );
                if( val != values[ i ] ) {
                    System.out.printf( "i=%d, b=%d, pos=%d, expected=%d, got=%d\n", i, bs[ i ], pos, values[ i ], val );
                    System.exit( 1 );
                }
                pos += bs[ i ];
            }

            for( int i = n - 1; i >= 0; --i ) {
                pos -= bs[ i ];
                ibs.position( pos );
                long val = ibs.readInt( bs[ i ] );
                if( val != values[ i ] ) {
                    System.out.printf( "i=%d, b=%d, pos=%d, expected=%d, got=%d\n", i, bs[ i ], pos, values[ i ], val );
                    System.exit( 1 );
                }
            }
        }

        // test readUnary
        {
            int n = ( 1 << 16 ) + 1;
            int[] values = new int[ n ];
            FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
            OutputBitStream obs = new OutputBitStream( oa, 0 );

            for( int i = 0; i < n; ++i ) {
                values[ i ] = rng.nextInt( 256 );
                obs.writeUnary( values[ i ] );
            }

            obs.close();
            oa.trim();

            FastInputBitStream ibs = new FastInputBitStream( oa.array );
            for( int i = 0; i < n; ++i ) {
                long val = ibs.readUnary();
                ;
                if( val != values[ i ] ) {
                    System.out.printf( "i=%d, expected=%d, got=%d\n", i, values[ i ], val );
                    System.exit( 1 );
                }
            }
        }

        // test readGolomb
        {
            int n = ( 1 << 16 ) + 1;
            int[] bs = new int[ n ];
            int[] values = new int[ n ];
            FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
            OutputBitStream obs = new OutputBitStream( oa, 0 );

            for( int i = 0; i < n; ++i ) {
                bs[ i ] = rng.nextInt( 1024 ) + 1;
                values[ i ] = rng.nextInt( 128 ) * bs[ i ] + rng.nextInt( bs[ i ] );
                obs.writeGolomb( values[ i ], bs[ i ] );
            }

            obs.close();
            oa.trim();

            FastInputBitStream ibs = new FastInputBitStream( oa.array );
            for( int i = 0; i < n; ++i ) {
                long val = ibs.readGolomb( bs[ i ] );
                if( val != values[ i ] ) {
                    System.out.printf( "i=%d, b=%d, expected=%d, got=%d\n", i, bs[ i ], values[ i ], val );
                    System.exit( 1 );
                }
            }
        }


        System.out.println( "Testing done" );

        // perfTest readGolomb
        {
            int n = ( 1 << 24 ) + 1;
            int runs = 15;
            int[] bs = new int[ n ];
            int[] values = new int[ n ];
            FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
            OutputBitStream obs = new OutputBitStream( oa, 0 );

            for( int i = 0; i < n; ++i ) {
                bs[ i ] = rng.nextInt( 1024 ) + 1;
                values[ i ] = rng.nextInt( 16 ) * bs[ i ] + rng.nextInt( bs[ i ] );
                obs.writeGolomb( values[ i ], bs[ i ] );
            }

            obs.close();
            oa.trim();

            long tick = 0;
            double elapsed;
            long tmp = 0;


            // FastInputBitStream
            for( int run = 0; run <= runs; ++run ) {
                if( run == 1 ) { // do not time the first run
                    tick = System.nanoTime();
                }

                FastInputBitStream ibs = new FastInputBitStream( oa.array );
                for( int i = 0; i < n; ++i ) {
                    tmp += ibs.readGolomb( bs[ i ] );
                }
            }
            do_not_optimize = tmp;
            elapsed = System.nanoTime() - tick;
            System.out.printf( "FastInputBitStream %.1f ns/integer\n", elapsed / ( runs * n ) );


            // InputBitStream
            for( int run = 0; run <= runs; ++run ) {
                if( run == 1 ) { // do not time the first run
                    tick = System.nanoTime();
                }

                InputBitStream ibs = new InputBitStream( oa.array );
                for( int i = 0; i < n; ++i ) {
                    tmp += ibs.readGolomb( bs[ i ] );
                }
                ibs.close();
            }
            do_not_optimize = tmp;
            elapsed = System.nanoTime() - tick;
            System.out.printf( "InputBitStream %.1f ns/integer\n", elapsed / ( runs * n ) );


            // Elias-Fano
            EliasFanoLongBigList ef = new EliasFanoLongBigList( LongIterators.wrap( IntIterators.wrap( values ) ) );
            long[] readValues = new long[ n ];
            for( int run = 0; run <= runs; ++run ) {
                if( run == 1 ) { // do not time the first run
                    tick = System.nanoTime();
                }
                ef.get( 0, readValues );
                tmp += readValues[ n - 1 ];
            }
            do_not_optimize = tmp;
            elapsed = System.nanoTime() - tick;
            System.out.printf( "EliasFanoLongBigList %.1f ns/integer\n", elapsed / ( runs * n ) );
        }
    }

}
