/**
 Copyright 2016, Yahoo Inc.
 Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 **/

package com.yahoo.semsearch.fastlinking.utils;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.TextPattern;
import org.xml.sax.SAXException;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the links of wikipedia pages
 * @author roi blanco
 */
public class ExtractLinks
implements IArticleFilter {
    private final static Pattern pattern = Pattern.compile("(.*)href=\"/(.*?)\"(.*)");
    private MutableString html = new MutableString();
    private MutableString title = new MutableString();
    protected final String baseURL = "http://en.wikipedia.org/wiki/";
    protected final String linkBaseURL = String.valueOf(this.baseURL) + "${title}";
    protected final String imageBaseURL = String.valueOf(this.baseURL) + "${image}";
    protected final FastBufferedReader wordReader = new FastBufferedReader();
    protected final MutableString word = new MutableString();
    protected final MutableString nonWord = new MutableString();
    protected static final TextPattern BRACKETS_CLOSED = new TextPattern((CharSequence)"]]");
    protected static final TextPattern BRACES_CLOSED = new TextPattern((CharSequence)"}}");
    protected final FileWriter writer;

    public ExtractLinks(String file) throws IOException {
        this.writer = new FileWriter(file);
    }

    public static void main(String[] arg) {
        if (arg.length < 2) {
            System.err.println(" USAGE java ExtractLinks  <inputFile> <output file> ");
        }
        try {
            ExtractLinks handler = new ExtractLinks(arg[1]);
            WikiXMLParser wxp = new WikiXMLParser(arg[0], (IArticleFilter)handler);
            wxp.parse();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WikiArticle article, Siteinfo siteinfo) throws SAXException {
        this.title.length(0);
        this.html.length(0);
        try {
            this.title.append(URLEncoder.encode(article.getTitle().replace(' ', '_'), "UTF-8"));
        }
        catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        }
        WikiModel wikiModel = new WikiModel(this.imageBaseURL, this.linkBaseURL);
        String plainText = wikiModel.render((ITextConverter)new PlainTextConverter(), article.getText());
        Set<String> links = wikiModel.getLinks();
        StringBuilder sb = new StringBuilder();
        for (String link : links) {
            sb.append( " " ).append( this.processLink( link ) );
        }
        sb.append("\t");
        String slinks = sb.toString();
        MutableString allText = new MutableString(plainText);
        this.word.length(0);
        this.nonWord.length(0);
        this.wordReader.setReader((Reader)new FastBufferedReader(allText));
        try {
            this.writer.append( this.title ).append( "\t" );
            this.writer.append(slinks);
            while (this.wordReader.next(this.word, this.nonWord)) {
                this.writer.append( this.word.toLowerCase() ).append( " " );
            }
            this.writer.append("\n");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String processLink(String input) {
        String jj = WikiModel.toHtml((String)("[[" + input + "]]"));
        Matcher matcher = pattern.matcher(jj);
        if (!matcher.find()) {
            return "";
        }
        jj = matcher.group(2);
        return jj;
    }
}