package it.cnr.isti.hpc;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.MinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Word2VecCompress implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory
            .getLogger( Word2VecCompress.class );

    private int numWords;
    private int vectorSize;
    private float quantizationFactor;
    private byte[] vectorStreams;
    private EliasFanoMonotoneLongBigList endpoints;
    private Object2LongFunction<? extends CharSequence> dictionary;
    private int[] golombModuli;

    public Word2VecCompress() {

    }

    public Word2VecCompress( int numWords, int vectorSize,
                             float quantizationFactor, byte[] vectorStreams,
                             EliasFanoMonotoneLongBigList endpoints,
                             Object2LongFunction<? extends CharSequence> dictionary,
                             int[] golombModuli ) {
        this.numWords = numWords;
        this.vectorSize = vectorSize;
        this.quantizationFactor = quantizationFactor;
        this.vectorStreams = vectorStreams;
        this.endpoints = endpoints;
        this.dictionary = dictionary;
        this.golombModuli = golombModuli;

    }

    // only for testing
    public int[] getInt( long idx ) {
        int[] ret = new int[ vectorSize ];
        long endpoint = endpoints.get( idx );
        FastInputBitStream ibs = new FastInputBitStream( vectorStreams );
        ibs.position( endpoint );

        for( int col = 0; col < vectorSize; ++col ) {
            ret[ col ] = Fast.nat2int( ibs.readGolomb( golombModuli[ col ] ) );
        }

        return ret;
    }

    public int[] getInt( String word ) {
        Long idx = word_id( word );
        if( idx == null ) {
            return null;
        }
        return getInt( idx );
    }

    public void get( long idx, float[] result, int offset ) {
        long endpoint = endpoints.get( idx );
        FastInputBitStream ibs = new FastInputBitStream( vectorStreams );
        ibs.position( endpoint );

        for( int col = 0; col < vectorSize; ++col ) {
            int val = Fast.nat2int( ibs.readGolomb( golombModuli[ col ] ) );
            result[ offset + col ] = ( ( ( float ) val ) + 0.5f * Integer.signum( val ) )
                    / quantizationFactor;
        }

    }

    public float[] get( long idx ) {
        float[] ret = new float[ vectorSize ];
        get( idx, ret, 0 );
        return ret;
    }

    public boolean get( String word, float[] result, int offset ) {
        Long idx = word_id( word );
        if( idx == null ) {
            return false;
        }
        get( idx, result, offset );
        return true;
    }

    public float[] get( String word ) {
        Long idx = word_id( word );
        if( idx == null ) {
            return null;
        }
        return get( idx );
    }

    public Long word_id( String word ) {
        return dictionary.get( word );
    }

    public int size() {
        return numWords;
    }

    public int dimensions() {
        return vectorSize;
    }

    public static void main( String[] args ) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP(
                Word2VecCompress.class.getName(),
                "Creates a compressed representation of quantized word2vec vectors",
                new Parameter[]{
                        new UnflaggedOption( "input", JSAP.STRING_PARSER, true,
                                "Input file" ),
                        new UnflaggedOption( "output", JSAP.STRING_PARSER,
                                false, "Compressed version" ),
                        new Switch( "check", JSAP.NO_SHORTFLAG, "check",
                                "Check correctness of output" ) } );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() )
            return;

        String input_filename = jsapResult.getString( "input" );
        String output_filename = jsapResult.getString( "output", null );

        int numWords, vectorSize;
        float quantizationFactor;
        List<String> indexToWord;
        //int[] entries;
        int[][] entries;
        long size;
        long[] columnAbsSum;

        ProgressLogger pl = new ProgressLogger( logger );

        try( final BufferedReader lines = new BufferedReader( new InputStreamReader( new FileInputStream(
                input_filename ), "UTF-8" ) ) ) {

            String[] header = lines.readLine().split( "\t" );
            numWords = Integer.parseInt( header[ 0 ] );
            vectorSize = Integer.parseInt( header[ 1 ] );
            quantizationFactor = Float.parseFloat( header[ 2 ] );

            pl.expectedUpdates = numWords;
            pl.start( "Reading the dictionary" );
            indexToWord = new ArrayList<>();

            ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<String>();
            Random r = new Random();
            for( int i = 0; i < numWords; ++i ) {
                pl.lightUpdate();
                String s = lines.readLine();//.trim();//unicode vs ascii
                if( s.charAt( s.length() - 1 ) == '\n' ) {
                    s = s.substring( 0, s.length() - 2 );
                }
                if( !stringSet.contains( s ) ) {
                    indexToWord.add( s );
                    stringSet.add( s );
                } else {
                    System.out.println( "dup <" + s + "> line " + i );
                    //horrible hack for duplicates
                    indexToWord.add( s + r.nextDouble() );
                }
            }
            pl.done();
            //System.out.println( numWords * vectorSize + " entries " + " nW " + numWords + " vS " + vectorSize  );
            //  entries = new int[ numWords * vectorSize ];
            columnAbsSum = new long[ vectorSize ];
            size = ( long ) numWords * vectorSize;
            entries = IntBigArrays.newBigArray( size );
            pl.expectedUpdates = numWords;
            pl.start( "Reading the vectors" );
            for( int i = 0; i < numWords; ++i ) {
                pl.lightUpdate();
                String line = lines.readLine();
                String[] lineEntries = line.split( " " );
                for( int col = 0; col < vectorSize; ++col ) {
                    int entry = Integer.parseInt( lineEntries[ col ] );
                    // entries[ i * vectorSize + col ] = entry;
                    IntBigArrays.add( entries, i * vectorSize + col, entry );
                    columnAbsSum[ col ] += Fast.int2nat( entry ) + 1;
                }
            }
            pl.done();

        }

        int[] golombModuli = new int[ vectorSize ];
        for( int col = 0; col < vectorSize; ++col ) {
            int m = 0;
            if( columnAbsSum[ col ] > numWords ) {
                double f = ( ( double ) numWords ) / columnAbsSum[ col ];
                m = ( int ) Math.ceil( Math.log( 2.0 - f ) / -Math.log( 1.0 - f ) );
            }
            golombModuli[ col ] = m;
        }

        // logger.debug("Golomb moduli {}", golombModuli);

//		numWords = indexToWord.size(); //TODO ?? hack for dups


        ShiftAddXorSignedStringMap dictionaryHash = new ShiftAddXorSignedStringMap(
                indexToWord.iterator(),
                new MinimalPerfectHashFunction.Builder<CharSequence>()
                        .keys( indexToWord )
                        .transform( TransformationStrategies.utf16() ).build() );
        int[] permutation = new int[ numWords ];
        for( int i = 0; i < numWords; ++i ) {
            int newPos = dictionaryHash.get( indexToWord.get( i ) ).intValue();
            permutation[ newPos ] = i;
        }

        FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
        OutputBitStream obs = new OutputBitStream( oa, 0 );
        final LongArrayList endpoints = new LongArrayList();

        pl.expectedUpdates = numWords;
        pl.start( "Compressing the vectors" );
        for( int i = 0; i < numWords; ++i ) {
            pl.lightUpdate();
            endpoints.add( obs.writtenBits() );
            int rowStart = permutation[ i ] * vectorSize;

            for( int col = 0; col < vectorSize; ++col ) {
                //int entry = entries[ rowStart + col ];
                int entry = IntBigArrays.get( entries, rowStart + col );
                obs.writeGolomb( Fast.int2nat( entry ), golombModuli[ col ] );

            }
        }
        pl.done();

        obs.close();
        while( oa.length() % 4 != 0 ) {
            oa.write( 0 ); // pad to int for FastInputBitStream
        }
        oa.trim();

        //double bps = 8.0 * oa.array.length / entries.length;
        double bps = 8.0 * oa.array.length / size;
        logger.info( "Overall vector bit streams: {} bytes, {} bps",
                oa.array.length, bps );
        System.out.println( bps );

        EliasFanoMonotoneLongBigList efEndpoints = new EliasFanoMonotoneLongBigList(
                endpoints );
        Word2VecCompress word2vec = new Word2VecCompress( numWords, vectorSize,
                quantizationFactor, oa.array, efEndpoints, dictionaryHash,
                golombModuli );
        if( output_filename != null ) {
            BinIO.storeObject( word2vec, output_filename );
        }

        if( jsapResult.getBoolean( "check" ) ) {
            pl.expectedUpdates = numWords;
            pl.start( "Checking the output" );

            for( int i = 0; i < numWords; ++i ) {
                pl.lightUpdate();
                int[] vec = word2vec.getInt( indexToWord.get( i ) );
                for( int col = 0; col < vectorSize; ++col ) {
                    //int expected = entries[ i * vectorSize + col ];
                    int expected = IntBigArrays.get( entries, i * vectorSize + col );
                    int got = vec[ col ];
                    if( expected != got ) {
                        logger.error( "Row {}, Column {}: Expected {}, got {}",
                                i, col, expected, got );
                        System.exit( 1 );
                    }
                }
            }
            pl.done();
        }
    }
}
