/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.hash;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.ints.IntBigArrayBigList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterable;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongIterators;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.MWHCFunction;
import it.unimi.dsi.sux4j.util.EliasFanoLongBigList;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.yahoo.semsearch.fastlinking.view.CandidatesInfo;
import com.yahoo.semsearch.fastlinking.view.Entity;
import com.yahoo.semsearch.fastlinking.view.StringAndCandidate;

/**
 * Uber class holding compressed aliases and features for entities and their stats
 *
 * @author roi blanco
 */
public class QuasiSuccinctEntityHash extends AbstractEntityHash implements Serializable {
    private final static Logger LOGGER = LoggerFactory.getLogger( QuasiSuccinctEntityHash.class );
    private final static int PER_CANDIDATE_FEATURES = 5; //QAF, QAT, QAC, LAF, LAT (ignoring MAF, MAT)
    private final static int PER_ENTITY_CANDIDATE_FEATURES = 3; //id, LAET, QAEF, (MAET)
    private final static int PER_ENTITY_FEATURES = 3; // QEF, LET, type, (MET)

    public final static int ALIASESPERBATCH = 10000000; //we use a number of different longbiglists to avoid overflowing
    private static final long serialVersionUID = 1L;
    public EliasFanoMonotoneLongBigList pointers[];
    public EliasFanoLongBigList values[];
    public final EliasFanoLongBigList entityValues;
    public final FrontCodedStringList entityNames;
    public CountAndRecordStats stats;

    public QuasiSuccinctEntityHash( Object2LongFunction<? extends CharSequence> hash, EliasFanoMonotoneLongBigList[] pointers, EliasFanoLongBigList[] values, EliasFanoLongBigList entityValues, FrontCodedStringList
            frontCodedStringList ) {
        super( hash );
        this.pointers = pointers;
        this.values = values;
        this.entityValues = entityValues;
        this.entityNames = frontCodedStringList;
    }

    /**
     * returns the candidates with their features for a given surface form
     */
    public CandidatesInfo getCandidatesInfo( String surfaceForm ) {
        final long id = hash.getLong( surfaceForm );
        return id != -1 ? candidatesInfo( id ) : null;
    }

    /**
     * returns the Entity entity for a given id
     *
     * @param id entity identifier
     * @return entity object for the id specified
     */
    public Entity getEntity( final long id ) {
        Entity e = new Entity();
        long u[] = new long[ PER_ENTITY_FEATURES ];
        entityValues.get( id * PER_ENTITY_FEATURES, u );
        e.QEF = ( int ) u[ 0 ];
        e.LET = ( int ) u[ 1 ];
        e.type = ( short ) u[ 2 ];
        e.id = ( int ) id;
        return e;
    }


    /**
     * returns the features in a CandidatesInfo class that belong to a particular index
     *
     * @param index where in the compressed stream we have to look up the info
     * @return candidates info object for the specified index
     */
    protected CandidatesInfo candidatesInfo( long index ) {
        final long startEnd[] = pointers[ ( int ) ( index / ALIASESPERBATCH ) ].get( index % ALIASESPERBATCH, new long[ 2 ] );
        final int numEntities = ( int ) ( ( startEnd[ 1 ] - startEnd[ 0 ] - PER_CANDIDATE_FEATURES ) / PER_ENTITY_CANDIDATE_FEATURES );
        long t[] = values[ ( int ) ( index / ALIASESPERBATCH ) ].get( startEnd[ 0 ], new long[ ( int ) ( startEnd[ 1 ] - startEnd[ 0 ] ) ] );
        long u[] = new long[ PER_ENTITY_FEATURES ];
        Entity[] e = new Entity[ numEntities ];
        for( int i = 0; i < numEntities; i++ ) {
            e[ i ] = new Entity();
            e[ i ].id = ( int ) t[ PER_CANDIDATE_FEATURES + i * PER_ENTITY_CANDIDATE_FEATURES ];
            e[ i ].LAET = ( int ) t[ PER_CANDIDATE_FEATURES + 1 + i * PER_ENTITY_CANDIDATE_FEATURES ];
            e[ i ].QAEF = ( int ) t[ PER_CANDIDATE_FEATURES + 2 + i * PER_ENTITY_CANDIDATE_FEATURES ];
            entityValues.get( e[ i ].id * PER_ENTITY_FEATURES, u );
            e[ i ].QEF = ( int ) u[ 0 ];
            // e[ i ].MET = (int) u[ 1 ];
            e[ i ].LET = ( int ) u[ 1 ];
            e[ i ].type = ( short ) u[ 2 ];
        }
        return new CandidatesInfo( e, ( int ) t[ 0 ], ( int ) t[ 1 ], ( int ) t[ 2 ], ( int ) t[ 3 ], ( int ) t[ 4 ] );//, (int) t[ 5 ] );
    }

    /**
     * Creates a QuasiSuccinctEntityHash from a datapack file. This class will compress everything: strings using
     * minimal perfect hash functions, entity identifiers, and all the entity features, provided tha
     * that every feature is an integer.
     *
     * @param args command line args; see -help
     * @throws Exception
     */
    public static void main( String[] args ) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP( QuasiSuccinctEntityHash.class.getName(), "Creates a MPHF from a file with the candidates info", new Parameter[]{
                new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "Input " + "file" ),
                new FlaggedOption( "entity2id", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'e', "entities", "TAB-separated entity names and corresponding ids" ),
                new FlaggedOption( "wikiThreshold", JSAP.INTEGER_PARSER, "0", JSAP.NOT_REQUIRED, 'w', "wikiThreshold", "Minimum number of anchors to store a candidate" ),
                new FlaggedOption( "queryThreshold", JSAP.INTEGER_PARSER, "0", JSAP.NOT_REQUIRED, 'q', "queryThreshold", "Minimum number of clicks to store a candidate" ),
                new FlaggedOption( "output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "Compressed version" ), }
        );
        JSAPResult jsapResult = jsap.parse( args );
        if( jsap.messagePrinted() ) return;
        final Iterable<StringAndCandidate> stringAndCandidates = FormatReader.stringAndCandidates( jsapResult.getString( "input" ), jsapResult.getInt( "queryThreshold" ), jsapResult.getInt( "wikiThreshold" ) );

        final BufferedReader linesC = new BufferedReader( new FileReader( jsapResult.getString( "entity2id" ) ) );
        int maxIndex = 0;
        String line;
        while( ( line = linesC.readLine() ) != null ) {
            String[] parts = line.split( "\t" );
            int x = Integer.parseInt( parts[ 1 ] );
            if( x > maxIndex ) maxIndex = x; //This is quicker than calling size() many times
        }
        linesC.close();
        final BufferedReader lines = new BufferedReader( new FileReader( jsapResult.getString( "entity2id" ) ) );

        // A list containing entity names at id positions
        ObjectArrayList<String> entityNames = new ObjectArrayList<>( maxIndex );
        LOGGER.info( "Storing entity names" );
        while( ( line = lines.readLine() ) != null ) {
            String[] parts = line.split( "\t" );
            try {
                final int index = Integer.parseInt( parts[ 1 ] );
                if( index >= entityNames.size() ) entityNames.size( index + 1 );
                final String oldValue = entityNames.set( index, parts[ 0 ] );
                if( oldValue != null ) LOGGER.warn( "Duplicate index " + index + " for names \"" + parts[ 0 ] + "\" and \"" + oldValue +
                        "\"" );
            } catch( NumberFormatException e ) {
                LOGGER.error( "Wrong line (skipping) --> " + line );
            }

        }
        LOGGER.info( "done" );
        lines.close();

        for( int i = 0; i < entityNames.size(); i++ )
            if( entityNames.get( i ) == null ) {
                //LOGGER.warn( "No entity name for index " + i );
                entityNames.set( i, "" ); // Fix for FCL
            }

        HashMap<Integer, DataOutputStream> valuesArray = new HashMap<Integer, DataOutputStream>();
        HashMap<Integer, LongArrayList> pointersArray = new HashMap<Integer, LongArrayList>();
        HashMap<Integer, File> tempFiles = new HashMap<Integer, File>();

        LongArrayList cutPoints = new LongArrayList();
        File tempFile = File.createTempFile( "values", "tempfile" );
        tempFile.deleteOnExit();
        int batchNumber = 0;
        tempFiles.put( batchNumber, tempFile );
        DataOutputStream values = new DataOutputStream( new FastBufferedOutputStream( new FileOutputStream( tempFile ) ) );
        cutPoints.add( 0 );
        valuesArray.put( batchNumber, values );
        pointersArray.put( batchNumber, cutPoints );

        long valuesSize = 0;
        final IntBigArrayBigList entityValues = new IntBigArrayBigList();
        entityValues.size( entityNames.size() * PER_ENTITY_FEATURES );
        ProgressLogger pl = new ProgressLogger( LOGGER );
        pl.itemsName = "aliases";
        int numberOfCandidates = 0;
        for( StringAndCandidate sc : stringAndCandidates ) {
            pl.lightUpdate();
            if( numberOfCandidates++ > ALIASESPERBATCH - 1 ) {
                numberOfCandidates = 1;
                batchNumber++;
                tempFile = File.createTempFile( "values", "tempfile" );
                tempFile.deleteOnExit();
                values.close();
                values = new DataOutputStream( new FastBufferedOutputStream( new FileOutputStream( tempFile ) ) );
                cutPoints = new LongArrayList();
                cutPoints.add( 0 );
                valuesArray.put( batchNumber, values );
                pointersArray.put( batchNumber, cutPoints );
                tempFiles.put( batchNumber, tempFile );
                valuesSize = 0;
            }

            CandidatesInfo ci = sc.candidatesInfo;
            values.writeInt( ( int ) ci.QAF );
            values.writeInt( ( int ) ci.QAT );
            values.writeInt( ( int ) ci.QAC );
            // values.writeInt( (int) ci.MAF ); QAF, QAT, QAC, MAF, MAT, LAF, LAT
            // values.writeInt( (int) ci.MAT );
            values.writeInt( ( int ) ci.LAF );
            values.writeInt( ( int ) ci.LAT );
            valuesSize += PER_CANDIDATE_FEATURES;
            for( Entity e : ci.entities ) {
                values.writeInt( e.id );
                //	values.writeInt( (int) e.MAET );
                values.writeInt( ( int ) e.LAET );
                values.writeInt( ( int ) e.QAEF );
                valuesSize += PER_ENTITY_CANDIDATE_FEATURES;
                entityValues.set( e.id * PER_ENTITY_FEATURES, ( int ) e.QEF );
                //	entityValues.set( e.id * PER_ENTITY_FEATURES + 1, (int) e.MET );
                entityValues.set( e.id * PER_ENTITY_FEATURES + 1, ( int ) e.LET );
                entityValues.set( e.id * PER_ENTITY_FEATURES + 2, e.type );

            }
            cutPoints.add( valuesSize );
        }
        values.close();
        pl.done();
        Iterable<CharSequence> surfaceForms = new Iterable<CharSequence>() {
            @Override
            public Iterator<CharSequence> iterator() {
                return new AbstractObjectIterator<CharSequence>() {
                    Iterator<StringAndCandidate> i = stringAndCandidates.iterator();

                    @Override
                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override
                    public String next() {
                        return i.next().surfaceForm;
                    }
                };
            }
        };


        ShiftAddXorSignedStringMap surfaceForm2Position = new ShiftAddXorSignedStringMap( surfaceForms.iterator(),
                new MWHCFunction.Builder<CharSequence>().keys( surfaceForms ).transform( TransformationStrategies.utf16() ).build() );

        EliasFanoLongBigList[] valuesA = new EliasFanoLongBigList[ tempFiles.size() ];
        EliasFanoMonotoneLongBigList[] cutPointsArray = new EliasFanoMonotoneLongBigList[ tempFiles.size() ];
        for( int i = 0; i < tempFiles.size(); i++ ) {
            final File xx = tempFiles.get( i );
            LongIterable lI = new LongIterable() {
                public LongIterator iterator() {
                    return LongIterators.wrap( BinIO.asIntIterable( xx ).iterator() );
                }
            };
            long lowerBound = Long.MAX_VALUE;
            LongIterator iterator = lI.iterator();
            while( iterator.hasNext() ) lowerBound = Math.min( lowerBound, iterator.nextLong() );

            cutPointsArray[ i ] = new EliasFanoMonotoneLongBigList( pointersArray.get( i ) );
            valuesA[ i ] = new EliasFanoLongBigList( lI.iterator(), lowerBound, true );
        }
        LOGGER.info( "#Batches= " + tempFiles.size() );
        QuasiSuccinctEntityHash quasiSuccinctEntityHash = new QuasiSuccinctEntityHash( surfaceForm2Position, cutPointsArray, valuesA, new EliasFanoLongBigList( entityValues ), new FrontCodedStringList( entityNames, 8,
                true ) );
        LOGGER.info( "Creating stats" );
        quasiSuccinctEntityHash.stats = CountAndRecordStats.createStats( quasiSuccinctEntityHash );
        BinIO.storeObject( quasiSuccinctEntityHash, jsapResult.getString( "output" ) );
        LOGGER.info( "...  done" );
        assert checkMap( jsapResult, stringAndCandidates );

    }

    /**
     * Checks if the map has been created correctly, by decompressing the data structure and checking it against the
     * original input file
     *
     * @param jsapResult command line params
     * @param stringAndCandidates the compressed features and strings
     * @return true if the test passes
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static boolean checkMap( JSAPResult jsapResult, Iterable<StringAndCandidate> stringAndCandidates ) throws IOException, ClassNotFoundException {
        QuasiSuccinctEntityHash hashCompressor = ( QuasiSuccinctEntityHash ) BinIO.loadObject( jsapResult.getString( "output" ) );
        int i = 0;
        for( StringAndCandidate sc : stringAndCandidates ) {
            final CandidatesInfo original = sc.candidatesInfo;
            final CandidatesInfo compressed = hashCompressor.candidatesInfo( i );
            assert original.QAF == compressed.QAF;
            assert original.QAT == compressed.QAT;
            //  assert original.MAF == compressed.MAF;
            //  assert original.MAT == compressed.MAT;
            assert original.QAC == compressed.QAC;
            assert original.LAF == compressed.LAF;
            assert original.LAT == compressed.LAT;
            for( int j = 0; j < original.entities.length; j++ ) {
                assert sc.candidatesInfo.entities[ j ].id == hashCompressor.candidatesInfo( i ).entities[ j ].id;
                assert sc.candidatesInfo.entities[ j ].type == hashCompressor.candidatesInfo( i ).entities[ j ].type;
                //	assert sc.candidatesInfo.entities[ j ].MAET == hashCompressor.candidatesInfo( i ).entities[ j ].MAET;
                assert sc.candidatesInfo.entities[ j ].LAET == hashCompressor.candidatesInfo( i ).entities[ j ].LAET;
                assert sc.candidatesInfo.entities[ j ].QEF == hashCompressor.candidatesInfo( i ).entities[ j ].QEF;
                assert sc.candidatesInfo.entities[ j ].QAEF == hashCompressor.candidatesInfo( i ).entities[ j ].QAEF;
                //	assert sc.candidatesInfo.entities[ j ].MET == hashCompressor.candidatesInfo( i ).entities[ j ].MET;
                assert sc.candidatesInfo.entities[ j ].LET == hashCompressor.candidatesInfo( i ).entities[ j ].LET;
            }
            i++;
        }
        return true;
    }

    @Override
    public CharSequence getEntityName( int id ) {
        return entityNames.get( id );
    }
}
