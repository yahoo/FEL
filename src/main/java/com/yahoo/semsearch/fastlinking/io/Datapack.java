/**
 * Copyright 2016, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.io;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URLEncoder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.map.HMapSIW;

public class Datapack extends Configured implements Tool {

    static final String outputEncoding = "UTF-8";
    private static final String SEPARATOR = Character.toString( '\u0001' );

    private static final String OUTPUT_OPTION = "output";
    private static final String MULTI_OUTPUT_OPTION = "multi";
    private static final String ANCHORMAP_OPTION = "amap";
    private static final String CFMAP_OPTION = "cfmap";
    private static final String NGRAM_OPTION = "ngram";

    @Override
    @SuppressWarnings( "static-access" )
    public int run( String[] args ) throws Exception {
        Options options = new Options();
        options.addOption( OptionBuilder.withArgName( "path" ).hasArg().withDescription( "output" ).create( OUTPUT_OPTION ) );
        options.addOption( OptionBuilder.withArgName( "path" ).hasArg().withDescription( "multi-output" ).create( MULTI_OUTPUT_OPTION ) );
        options.addOption( OptionBuilder.withArgName( "path" ).hasArg().withDescription( "output for anchor map" ).create( ANCHORMAP_OPTION ) );
        options.addOption( OptionBuilder.withArgName( "path" ).hasArg().withDescription( "output for anchor cf map" ).create( CFMAP_OPTION ) );
        options.addOption( OptionBuilder.withArgName( "ngram" ).hasArg().withDescription( "chunk into ngram" ).create( NGRAM_OPTION ) );

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse( options, args );
        } catch( ParseException exp ) {
            System.err.println( "Error parsing command line: " + exp.getMessage() );
            return -1;
        }

        if( !cmdline.hasOption( OUTPUT_OPTION ) || !cmdline.hasOption( ANCHORMAP_OPTION ) || !cmdline.hasOption( CFMAP_OPTION ) ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( this.getClass().getName(), options );
            ToolRunner.printGenericCommandUsage( System.out );
            return -1;
        }

        merge( cmdline.getOptionValue( ANCHORMAP_OPTION ), cmdline.getOptionValue( CFMAP_OPTION ), cmdline.getOptionValue( MULTI_OUTPUT_OPTION ), cmdline.getOptionValue( OUTPUT_OPTION ), cmdline.getOptionValue(
                NGRAM_OPTION ) );

        return 0;
    }

    private void merge( String anchorMapPath, String dfMapPath, String multiple_out, String out, String ngram ) throws IOException {

        JobConf conf = new JobConf( getConf(), Datapack.class );
        FileSystem fs = FileSystem.get( conf );

        BufferedWriter anchorsDataOut;
        BufferedWriter anchorsTSVOut;

        Boolean multiple_output = ( multiple_out != null && multiple_out.equalsIgnoreCase( "true" ) );
        Boolean ngram_output = ( ngram != null && ngram.equalsIgnoreCase( "true" ) );

        if( !multiple_output ) {
            anchorsDataOut = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( out ), outputEncoding ) );
            anchorsTSVOut = null;
        } else {
            anchorsDataOut = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( out + ".dat" ), outputEncoding ) );
            anchorsTSVOut = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( out + ".tsv" ), outputEncoding ) );
        }

        // Loop over anchors
        MapFile.Reader anchorMapReader = new MapFile.Reader(new Path(anchorMapPath + "/part-00000"), conf);
        MapFile.Reader dfMapReader = new MapFile.Reader(new Path(dfMapPath + "/part-00000"), conf);

        /*FileStatus[] status = fs.listStatus( new Path( dfMapPath ) );  // you need to pass in your hdfs path
        for( FileStatus fileStatus : status ) {
            if( !fileStatus.getPath().toString().contains( "part-" )) continue;
            MapFile.Reader dfMapReader = new MapFile.Reader( fileStatus.getPath(), conf );
*/
            Text akey = new Text();
            Text dkey = new Text();
            IntWritable df = new IntWritable();
            HMapSIW map = new HMapSIW();

		while (anchorMapReader.next(akey, map)) {

			// since they are both sorted we can just iterate over both
			// TODO if need be, artificially add a 0 count to unseen anchors
			dfMapReader.next(dkey, df);
			while (!akey.toString().equalsIgnoreCase(dkey.toString())) {
				//System.err.println("Mismatch: '" + akey + "' and '" + dkey + "'");
				anchorMapReader.next(akey, map);
			}
			String l = akey.toString();

//            while( dfMapReader.next( dkey, df ) ) {

  //              String l = dkey.toString();
                if (l.trim().length() < 2)
                	continue;

                StringBuilder targets = new StringBuilder();
                int total = 0;
                for( String target : map.keySet() ) {

                    int count = map.get( target );
                    total += count;

                    String entity = URLEncoder.encode( target.replaceAll( " ", "_" ), "UTF-8" );

                    targets.append( entity );
                    targets.append( SEPARATOR );
                    targets.append( Integer.toString( count ) );
                    targets.append( "\t" );

                }

                if( StringUtils.isNumeric( l ) && total < 2 ) continue;

                
//System.err.println("targets " + targets);
                if( targets.length() < 2) continue;
                if( !ngram_output ) {
                    anchorsDataOut.write( l );
                    anchorsDataOut.write( SEPARATOR );
                    anchorsDataOut.write( Integer.toString( df.get() ) );
                    anchorsDataOut.write( SEPARATOR );
                    anchorsDataOut.write( Integer.toString( total ) );
                    anchorsDataOut.write( "\t" );
                    anchorsDataOut.write( targets.substring( 0, targets.length() - 1 ) );
                    anchorsDataOut.write( "\n" );
                    anchorsDataOut.flush();

                    if( multiple_output ) {
                        for( String target : map.keySet() ) {
                            int count = map.get( target );
                            String entity = URLEncoder.encode( target.replaceAll( " ", "_" ), "UTF-8" );
                            anchorsTSVOut.write( l );
                            anchorsTSVOut.write( "\t" );
                            anchorsTSVOut.write( Integer.toString( df.get() ) );
                            anchorsTSVOut.write( "\t" );
                            anchorsTSVOut.write( Integer.toString( total ) );
                            anchorsTSVOut.write( "\t" );
                            anchorsTSVOut.write( entity );
                            anchorsTSVOut.write( "\t" );
                            anchorsTSVOut.write( Integer.toString( count ) );
                            anchorsTSVOut.write( "\n" );
                            anchorsTSVOut.flush();
                        }
                    }
                } else {
                    String parts[] = l.split( "\\s+" );
                    for( int i = 0; i < parts.length; i++ ) {
                        StringBuilder sb = new StringBuilder();
                        for( int j = i; j < parts.length; j++ ) {
                            sb.append( parts[ j ] );
                            String ss = sb.toString();
                            anchorsDataOut.write( ss );
                            anchorsDataOut.write( SEPARATOR );
                            anchorsDataOut.write( Integer.toString( df.get() ) );
                            anchorsDataOut.write( SEPARATOR );
                            anchorsDataOut.write( Integer.toString( total ) );
                            anchorsDataOut.write( "\t" );
                            anchorsDataOut.write( targets.substring( 0, targets.length() - 1 ) );
                            anchorsDataOut.write( "\n" );
                            anchorsDataOut.flush();
                            if( multiple_output ) {
                                for( String target : map.keySet() ) {
                                    int count = map.get( target );
                                    String entity = URLEncoder.encode( target.replaceAll( " ", "_" ), "UTF-8" );
                                    anchorsTSVOut.write( ss );
                                    anchorsTSVOut.write( "\t" );
                                    anchorsTSVOut.write( Integer.toString( df.get() ) );
                                    anchorsTSVOut.write( "\t" );
                                    anchorsTSVOut.write( Integer.toString( total ) );
                                    anchorsTSVOut.write( "\t" );
                                    anchorsTSVOut.write( entity );
                                    anchorsTSVOut.write( "\t" );
                                    anchorsTSVOut.write( Integer.toString( count ) );
                                    anchorsTSVOut.write( "\n" );
                                    anchorsTSVOut.flush();
                                }
                                sb.append( " " );
                            }
                        }
                    }
                }
            }
            dfMapReader.close();
        //}

        anchorsDataOut.close();

        if( multiple_output ) {
            anchorsTSVOut.close();
        }

        //anchorMapReader.close();

        fs.close();

    }

    public Datapack() {
    }

    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run( new Datapack(), args );
        System.exit( res );
    }
}
