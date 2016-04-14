

/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.yahoo.semsearch.fastlinking.io;

import info.bliki.wiki.model.WikiModel;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * An English page from Wikipedia. Extended version from https://lintool.github.io/Cloud9/
 * @author Peter Exner
 * @author Ferhan Ture
 */
public class EnglishWikipediaPage extends WikipediaPage {
	/**
	 * Language dependent identifiers of disambiguation, redirection, and stub pages.
	 */
	private static final String IDENTIFIER_REDIRECTION_UPPERCASE = "#REDIRECT";
	private static final String IDENTIFIER_REDIRECTION_LOWERCASE = "#redirect";
	private static final String IDENTIFIER_REDIRECTION_CAMELCASE = "#Redirect";
	private static final String IDENTIFIER_STUB_TEMPLATE = "stub}}";
	private static final String IDENTIFIER_STUB_WIKIPEDIA_NAMESPACE = "Wikipedia:Stub";

	// http://en.wikipedia.org/w/index.php?title=Houston_(disambiguation)&action=edit&editintro=Template:Disambig_editintro
	// {{disambiguation|geo|surname|given name}}
	//	private static final Pattern disambPattern = Pattern.compile("\\{\\{disambig\\w*\\}\\}", Pattern.CASE_INSENSITIVE);
	//	private static final Pattern disambPattern2 = Pattern.compile("\\(disambiguation\\)$");

	// See http://en.wikipedia.org/wiki/Category:Disambiguation_message_boxes
	// See http://en.wikipedia.org/wiki/Template:Disambiguation
	private static final String[] _patterns = { "\\{\\{disambig\\|?\\w*\\}\\}", "{{DISAMBIGUATION|", "|DISAMBIGUATION}}", "{{Geodis", "{{Hndis",
		"{{hndis-cleanup}}", "{{Letter-NumberCombDisambig", "{{Numberdis", "{{Mil-unit-dis", "{{Disambig-plants", "{{disambig|geo}}" };//"{{dmbox"
	private static final Pattern[] patterns = new Pattern[_patterns.length];
	static {
		patterns[0] = Pattern.compile(_patterns[0], Pattern.CASE_INSENSITIVE);

		for (int i = 1; i < patterns.length; i++) {
			patterns[i] = Pattern.compile(Pattern.quote(_patterns[i]), Pattern.CASE_INSENSITIVE);
		}
	}

	public static final boolean _isDisambiguation(String content) {

		for (Pattern p : patterns)
			if (p.matcher(content).find())
				return true;

		return false;

	}

	private static final String LANGUAGE_CODE = "en";

	/**
	 * Start delimiter of the revision, which is &lt;<code>revision</code>&gt;.
	 */
	protected static final String XML_START_TAG_REVISION = "<timestamp>";

	/**
	 * End delimiter of the namespace, which is &lt;<code>/revision</code>&gt;.
	 */
	protected static final String XML_END_TAG_REVISION = "</timestamp>";

	/**
	 * Infobox identifier, which is <code>{{Infobox</code>.
	 */
	String INFOBOX_CONST_STR = "{{Infobox";

	/**
	 * Citation identifier, which is <code>{{cite </code>.
	 */
	String CITE_CONST_STR = "{{cite ";
	// String CITE_CONST_STR = "&lt;ref ";

	/**
	 * Section heading identifier, which is &lt;<code>h2</code>&gt;.
	 */
	String SECTION_CONST_STR = "<h2>";

	/**
	 * Category identifier, which is <code>[[Category:</code>.
	 */
	String CATEGORY_CONST_STR = "[[Category:";

	private String revision;
	private Date revisionDate = null;
	private int infoboxCount;
	private int sectionCount;
	private int categoryCount;
	private int citationCount;
	private int exLinksCount;

	/**
	 * Creates an empty <code>EnglishWikipediaPage</code> object.
	 */
	public EnglishWikipediaPage() {
		super();
	}

	@Override
	protected void processPage(String s) {
		this.language = LANGUAGE_CODE;

		// parse out title
		int start = s.indexOf(XML_START_TAG_TITLE);
		int end = s.indexOf(XML_END_TAG_TITLE, start);
		this.title = StringEscapeUtils.unescapeHtml(s.substring(start + 7, end)).trim();

		// determine if article belongs to the article namespace
		start = s.indexOf(XML_START_TAG_NAMESPACE);
		end = s.indexOf(XML_END_TAG_NAMESPACE);
		this.isArticle = start == -1 ? true : s.substring(start + 4, end).trim().equals("0");
		// add check because namespace tag not present in older dumps

		// parse out the document id
		start = s.indexOf(XML_START_TAG_ID);
		end = s.indexOf(XML_END_TAG_ID);
		this.mId = s.substring(start + 4, end);

		// parse out actual text of article
		this.textStart = s.indexOf(XML_START_TAG_TEXT);
		this.textEnd = s.indexOf(XML_END_TAG_TEXT, this.textStart);

		// determine if article is a disambiguation, redirection, and/or stub page.
		//		this.isDisambig = disambPattern.matcher(page).find() || disambPattern2.matcher(this.title).find();
		this.isDisambig = _isDisambiguation(page);

		this.isRedirect = s.substring(this.textStart + XML_START_TAG_TEXT.length(),
				this.textStart + XML_START_TAG_TEXT.length() + IDENTIFIER_REDIRECTION_UPPERCASE.length()).compareTo(IDENTIFIER_REDIRECTION_UPPERCASE) == 0
				|| s.substring(this.textStart + XML_START_TAG_TEXT.length(),
						this.textStart + XML_START_TAG_TEXT.length() + IDENTIFIER_REDIRECTION_CAMELCASE.length()).compareTo(IDENTIFIER_REDIRECTION_CAMELCASE) == 0
						|| s.substring(this.textStart + XML_START_TAG_TEXT.length(),
								this.textStart + XML_START_TAG_TEXT.length() + IDENTIFIER_REDIRECTION_LOWERCASE.length()).compareTo(IDENTIFIER_REDIRECTION_LOWERCASE) == 0;

		this.isStub = s.indexOf(IDENTIFIER_STUB_TEMPLATE, this.textStart) != -1 || s.indexOf(IDENTIFIER_STUB_WIKIPEDIA_NAMESPACE) != -1;

		// get revision
		start = s.indexOf(XML_START_TAG_REVISION);
		end = s.indexOf(XML_END_TAG_REVISION, start);
		this.revision = s.substring(start + 11, end);

		// get revision as Date
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		try {
			this.revisionDate = dateFormat.parse(this.revision);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		// count number of infoboxes
		this.infoboxCount = countObject(s, INFOBOX_CONST_STR);

		// count number of categories
		this.categoryCount = countObject(s, CATEGORY_CONST_STR);

		// count number of citations
		this.citationCount = countObject(s, CITE_CONST_STR);

		String html;

		if (textStart == -1 || textEnd == -1 || textStart + 27 < 0) {
			html = "";
		} else {
			WikiModel wikiModel = new WikiModel("image::", "external::");
			wikiModel.setUp();
			try {
				html = wikiModel.render(s.substring(textStart + 27, textEnd));
				// System.err.println(html);
			} catch (StringIndexOutOfBoundsException e) {
				e.printStackTrace();
				System.err.println(s);
				html = "";
			}
			wikiModel.tearDown();
		}

		// count number of sections
		this.sectionCount = countObject(html, SECTION_CONST_STR);

		// count number of external links
		this.exLinksCount = countExternalLinks(html);

	}

	private int countExternalLinks(String html) {
		String ex = "<h2><span class=\"mw-headline\" id=\"External_links\">External links</span></h2>";
		int startPos = html.indexOf(ex);
		if (startPos < 0)
			return 0;

		int endPos = html.indexOf("</ul>", startPos + ex.length());
		if (endPos < 0)
			return 0;

		String block = html.substring(startPos, endPos);
		return countObject(block, "<li>");

	}

	private int countObject(String s, String obj) {

		int startPos = 0;
		int count = 0;

		while ((startPos = s.indexOf(obj, startPos)) > 0) {
			count++;
			startPos = startPos + obj.length();
		}

		return count;

	}

	public String getRevision() {
		return revision;
	}

	public Date getRevisionDate() {
		return revisionDate;
	}

	public int getInfoboxCount() {
		return infoboxCount;
	}

	public int getSectionCount() {
		return sectionCount;
	}

	public int getCategoryCount() {
		return categoryCount;
	}

	public int getCitationCount() {
		return citationCount;
	}

	public int getExternalLinksCount() {
		return exLinksCount;
	}

}
