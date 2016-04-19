/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/
package com.yahoo.semsearch.fastlinking;

import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash;
import com.yahoo.semsearch.fastlinking.view.EmptyContext;
import com.yahoo.semsearch.fastlinking.view.EntityContext;
import com.yahoo.semsearch.fastlinking.w2v.LREntityContext;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;
import java.util.List;


/**
 * Created by aasishkp on 9/30/15.
 *
 * This class is wrapper for FastEntityLinker and EntityContextFastEntityLinker. It allows applications using FEL as a library
 * to directly call EntityLinkWrapper and get Results irrespective of the linker being used.
 */
public class EntityLinkerWrapper {

    private QuasiSuccinctEntityHash hash;
    private EntityContextFastEntityLinker linker = null;
    private FastEntityLinker fel = null;


    /**
     * Use entityContextFastEntityLinker when following three arguments are passed
     * @param hashFile
     * @param unigramFile
     * @param entityFile
     */
    public EntityLinkerWrapper(String hashFile, String unigramFile, String entityFile) {
        try {
            EntityContext queryContext;
            hash = (QuasiSuccinctEntityHash) BinIO.loadObject(hashFile);
            queryContext = new LREntityContext(unigramFile, entityFile, hash);
            linker = new EntityContextFastEntityLinker(hash, hash.stats, queryContext);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Use FastEntityLinker when only hashfile is passed
     * @param hashFile
     */
    public EntityLinkerWrapper(String hashFile) {
        try {
            QuasiSuccinctEntityHash hash = (QuasiSuccinctEntityHash) BinIO.loadObject(hashFile);

            fel = new FastEntityLinker(hash, hash.stats, new EmptyContext());

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * This method returns a string of results for given query and @nbest limits number of entities to be generated
     * @param query
     * @param nBest
     * @return
     */
    public String getResults(String query, double nBest) {
        //List<String> strResults = new ArrayList<String>();
        StringBuilder strResults = new StringBuilder("");

        List<FastEntityLinker.EntityResult> results = null;
        if (linker != null) {

            //results = this.linker.getResults(query, threshold);
            try {
                results = this.linker.getResultsGreedy(query, (int) nBest);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else {
            //results = this.fel.getResults(query, threshold);
            try {
                results = this.fel.getResultsGreedy(query, (int) nBest);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //System.err.println(results.size());
        int count = 0;
        if (results == null) {
            return "";
        }
        for (FastEntityLinker.EntityResult er : results) {
            count += 1;

            if (er != null && this.hash != null) {

                strResults.append(er.text.toString() + "\t" +
                                  String.valueOf(er.score) + "\t" +
                                  String.valueOf(this.hash.getEntity(er.id).type));

                strResults.append("\n");
            }
            //System.err.println("here");
        }

        return strResults.toString();
    }
}
