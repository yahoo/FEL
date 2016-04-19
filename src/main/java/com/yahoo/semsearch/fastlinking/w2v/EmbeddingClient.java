/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.w2v;

import java.util.Hashtable;
import java.util.Vector;

import helma.xmlrpc.XmlRpc;
import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;

/**
 * Created by aasishkp on 3/9/16.
 */
public class EmbeddingClient {

    private final static String server_url =
        "http://localhost:8999/";

    public static void main (String [] args) {
        try {

            XmlRpc.setDriver("org.apache.xerces.parsers.SAXParser");
            // Create an object to represent our server.
            XmlRpcClient server = new XmlRpcClient(server_url);

            // Build our parameter list.
            Vector params = new Vector();
            params.addElement(new Integer(5));
            params.addElement(new Integer(3));

            // Call the server, and get our result.
            Hashtable result =
                (Hashtable) server.execute("sample.sumAndDifference", params);
            int sum = ((Integer) result.get("sum")).intValue();
            int difference = ((Integer) result.get("difference")).intValue();

            // Print out our result.
            System.out.println("Sum: " + Integer.toString(sum) +
                               ", Difference: " +
                               Integer.toString(difference));

            String[] phrases = new String[]{"the life of brian", "this is a test phrase"};
            String[] entities = new String[]{"_WIKI_Monty_Python", "_WIKI_John_Cleese", "_WIKI_Brad_Pitt"};


            params.clear();

            params = new Vector();

            params.addElement(entities[0]);
            params.addElement(entities[1]);
            result = (Hashtable) server.execute("sample.entity2EntitySimilarity", params);

            System.out.println(((Double) result.get("similarity")).doubleValue());

            params.clear();

            params.addElement(entities[0]);
            result = (Hashtable) server.execute("sample.getVector", params);

/*
            for(String entityA: entities){
                for (String entityB: entities){
                    params = new Vector();
                    params.addElement(entityA);
                    params.addElement(entityB);
                    double entitySimilarity = (double) server.execute("sample.entity2EntitySimilarity", params);
                    System.out.println("similarity for " + entityA + " " + entityB + ": "+ entitySimilarity);
                }
            }
*/



        } catch (XmlRpcException exception) {
            System.err.println("JavaClient: XML-RPC Fault #" +
                               Integer.toString(exception.code) + ": " +
                               exception.toString());
        } catch (Exception exception) {
            System.err.println("JavaClient: " + exception.toString());
        }
    }

}
