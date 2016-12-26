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
 */
public class EntityLinkerWrapper {

    private QuasiSuccinctEntityHash hash;
    private EntityContextFastEntityLinker linker = null;
    private FastEntityLinker fel = null;

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

    public String getResults(String query, double threshold) {
        //List<String> strResults = new ArrayList<String>();
        StringBuilder strResults = new StringBuilder("");

        List<FastEntityLinker.EntityResult> results = null;
        if (linker != null) {
            results = this.linker.getResultsGreedy(query, (int) threshold);
        } else {
            results = this.fel.getResultsGreedy(query, (int) threshold);
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
//                strResults.append(er.text.toString() + "\t" + er.score + "\t" +
//                        String.valueOf(er.s.getStartOffset()) + "\t" +
//                        String.valueOf(er.s.getEndOffset()) + "\t" +
//                        String.valueOf(er.s.getSpan()) + "\t" +
//                        String.valueOf(this.hash.getEntity(er.id).type));
                strResults.append("\n");
            }
            //System.err.println("here");
        }

        return strResults.toString();
    }
}
