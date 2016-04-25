/**
 * Copyright 2016, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import com.martiansoftware.jsap.*;
import it.cnr.isti.hpc.FastInputBitStream;
import it.cnr.isti.hpc.Word2VecCompress;
import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.LongArrayList;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Compressed word2vec-like format word vectors (previously quantized with @see com.yahoo.semsearch.fastlinking.w2v.Quantizer  This class performs several passes through the input data, because the original
 * Word2VecCompress implementation doesn't scale for many million entries.
 * Word strings are hashed and quantized vectors are golomb coded
 *
 * @author roi blanco
 */
public class EfficientWord2VecCompress extends Word2VecCompress {
    private static final Logger logger = LoggerFactory.getLogger( EfficientWord2VecCompress.class );

    public static void main( String[] args ) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP( Word2VecCompress.class.getName(), "Creates a compressed representation of quantized word2vec vectors", new Parameter[]{ new UnflaggedOption( "input", JSAP.STRING_PARSER, true,
                "Input file" ), new UnflaggedOption( "output", JSAP.STRING_PARSER, false, "Compressed version" ), new Switch( "check", JSAP.NO_SHORTFLAG, "check", "Check correctness of output" ) } );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;

        String input_filename = jsapResult.getString( "input" );
        String output_filename = jsapResult.getString( "output", null );

        int numWords, vectorSize;
        float quantizationFactor;
        List<String> indexToWord;
        long size;
        long[] columnAbsSum;

        ProgressLogger pl = new ProgressLogger( logger );

        try( final BufferedReader lines = new BufferedReader( new InputStreamReader( new FileInputStream( input_filename ), "UTF-8" ) ) ) {

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
                if( s.length() > 0 ) {
                    if( s.charAt( s.length() - 1 ) == '\n' ) {
                        s = s.substring( 0, s.length() - 2 );
                    }
                    if( !stringSet.contains( s ) ) {
                        indexToWord.add( s );
                        stringSet.add( s );
                    } else {
                        System.out.println( "dup <" + s + "> line " + i );
                        indexToWord.add( s + r.nextDouble() );
                    }
                }else{
                    indexToWord.add( "<<<<<<VOID>>>>>" + r.nextDouble() );
                }
            }
            pl.done();
            columnAbsSum = new long[ vectorSize ];
            size = ( long ) numWords * vectorSize;
            pl.expectedUpdates = numWords;
            pl.start( "Reading the vectors" );
            String preLin = "";
            for( int i = 0; i < numWords; ++i ) {
                pl.lightUpdate();
                String line = lines.readLine();
                try {
                    String[] lineEntries = line.split( " " );
                    for( int col = 0; col < vectorSize; ++col ) {
                        int entry = Integer.parseInt( lineEntries[ col ] );
                        columnAbsSum[ col ] += Fast.int2nat( entry ) + 1;
                    }
                } catch( Exception e ) {
                    System.err.println( "[ERROR] at line " + i + " : " + line + " word " + indexToWord.get( i ) );
                    e.printStackTrace();
                    System.exit( -1 );
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


        ShiftAddXorSignedStringMap dictionaryHash = new ShiftAddXorSignedStringMap( indexToWord.iterator(), new MinimalPerfectHashFunction.Builder<CharSequence>().keys( indexToWord ).transform(
                TransformationStrategies.utf16() ).build() );

        int[] permutation = new int[ numWords ];
        for( int i = 0; i < numWords; ++i ) {
            int newPos = dictionaryHash.get( indexToWord.get( i ) ).intValue();
            permutation[ newPos ] = i;
        }

        FastByteArrayOutputStream oa = new FastByteArrayOutputStream();
        OutputBitStream obs = new OutputBitStream( oa, 0 );
        LongArrayList endpoints = new LongArrayList();

        pl.expectedUpdates = numWords;
        pl.start( "First-pass compressing the vectors" );
        try( final BufferedReader lines = new BufferedReader( new InputStreamReader( new FileInputStream( input_filename ), "UTF-8" ) ) ) {
            lines.readLine();//header
            for( int i = 0; i < numWords; ++i ) {
                lines.readLine(); //skip all the words
            }

            for( int i = 0; i < numWords; ++i ) {
                pl.lightUpdate();
                endpoints.add( obs.writtenBits() );
                //int rowStart = permutation[ i ] * vectorSize;
                String line = lines.readLine();
                String[] lineEntries = line.split( " " );
                for( int col = 0; col < vectorSize; ++col ) {
                    int entry = Integer.parseInt( lineEntries[ col ] );
                    //int entry = entries[ rowStart + col ];
                    //int entry = IntBigArrays.get( entries, rowStart + col );
                    obs.writeGolomb( Fast.int2nat( entry ), golombModuli[ col ] );
                }
            }
            pl.done();

            obs.close();
            while( oa.length() % 4 != 0 ) {
                oa.write( 0 ); // pad to int for FastInputBitStream
            }
            oa.trim();
        }
        //double bps = 8.0 * oa.array.length / entries.length;
        double bps = 8.0 * oa.array.length / size;
        logger.info( "Overall vector bit streams: {} bytes, {} bps", oa.array.length, bps );


        FastByteArrayOutputStream noa = new FastByteArrayOutputStream();
        OutputBitStream nobs = new OutputBitStream( noa, 0 );
        pl.expectedUpdates = numWords;
        pl.start( "Second pass re-compressing the vectors" );

        final LongArrayList nendpoints = new LongArrayList();
        FastInputBitStream ibs = new FastInputBitStream( oa.array );
        for( int i = 0; i < numWords; i++ ) { //re-code
            pl.lightUpdate();
            nendpoints.add( nobs.writtenBits() );
            long endpoint = endpoints.get( permutation[ i ] );
            ibs.position( endpoint );
            for( int col = 0; col < vectorSize; ++col ) {
                int val = Fast.nat2int( ibs.readGolomb( golombModuli[ col ] ) );
                nobs.writeGolomb( Fast.int2nat( val ), golombModuli[ col ] );
            }
        }
        pl.done();
        nobs.close();
        while( noa.length() % 4 != 0 ) {
            noa.write( 0 ); // pad to int for FastInputBitStream
        }
        noa.trim();
        endpoints = nendpoints;
        oa = noa;

        EliasFanoMonotoneLongBigList efEndpoints = new EliasFanoMonotoneLongBigList( endpoints );
        Word2VecCompress word2vec = new Word2VecCompress( numWords, vectorSize, quantizationFactor, oa.array, efEndpoints, dictionaryHash, golombModuli );
        if( output_filename != null ) {
            BinIO.storeObject( word2vec, output_filename );
        }

        if( jsapResult.getBoolean( "check" ) ) {
            pl.expectedUpdates = numWords;
            pl.start( "Checking the output" );

            try( final BufferedReader lines = new BufferedReader( new InputStreamReader( new FileInputStream( input_filename ), "UTF-8" ) ) ) {
                lines.readLine();//header
                for( int i = 0; i < numWords; ++i ) {
                    lines.readLine(); //skip all the words
                }
                for( int i = 0; i < numWords; ++i ) {
                    pl.lightUpdate();
                    int[] vec = word2vec.getInt( indexToWord.get( i ) );
                    String line = lines.readLine();
                    String[] lineEntries = line.split( " " );
                    for( int col = 0; col < vectorSize; ++col ) {
                        int expected = Integer.parseInt( lineEntries[ col ] );
                        int got = vec[ col ];
                        if( expected != got ) {
                            logger.error( "Row {}, Column {}: Expected {}, got {}", i, col, expected, got );
                            System.exit( 1 );
                        }
                    }
                }
                pl.done();
            }
        }
    }
}
