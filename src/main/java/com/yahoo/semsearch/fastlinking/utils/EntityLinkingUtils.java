/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType.file;

/**
 * Created by aasishkp on 12/30/15.
 */
public class EntityLinkingUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntityLinkingUtils.class);

    public static String hexadecimalToChar(String hexadecimalString) {
        try {
            return URLDecoder.decode(hexadecimalString, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("Caught encoding error ", e);

        }
        return hexadecimalString;
    }

    /** @return the first element in the Collection, or Optional.empty() */
    public static <T> Optional<T> getFirstElement(Collection<T> list) {
        return list.stream().findFirst();
    }

    public static List<String> readEntityStrings(String filename){

        List<String> entityStrings = new ArrayList<String>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                entityStrings.add(line.replace("\n", "").replace("\r", ""));
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }

        return entityStrings;
    }
}

