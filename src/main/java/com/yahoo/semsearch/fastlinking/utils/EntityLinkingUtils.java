package com.yahoo.semsearch.fastlinking.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

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


}

