package ngram;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Book {
	private String headerText, bodyText, author, year;
	private int ngramCount;

	public Book(String rawText, int ngramCount) {
		this.ngramCount = ngramCount;
		// #TODO#: Split rawText into headerText and bodyText
		// Hint: Look for a specific pattern that separates metadata from book content

		// #TODO#: Call appropriate methods to initialize other class variables
	}

	private String parseAuthor(String headerText) {
		// #TODO#: Extract author's last name from headerText
		// Hint: Look for a specific pattern that indicates the author's name
		// (check parseYear() for guidelines)
	}

	private String parseYear(String headerText) {
		// #TODO#: Extract release year from headerText
		// Hint: Look for "Release Date" and then extract the year
		Pattern yearPattern = Pattern.compile("YOUR_PATTERN_HERE");
		Matcher yearMatcher = yearPattern.matcher(headerText);
		if (yearMatcher.find()) {
			// #TODO#: Extract and return the year
		}
		return "Unknown";
	}

	public String getBookAuthor() {
		return author;
	}

	public String getBookYear() {
		return year;
	}

	public String getBookHeader() {
		return headerText;
	}

	public String getBookBody() {
		return bodyText;
	}

	private String formatBook(String bookText) {
		if (ngramCount < 2) {
			// #TODO#: Format book text for unigram
			// Hint: Consider case, punctuation, and special characters
		} else {
			// #TODO#: Format book text for bigram
			// Hint: Consider sentence boundaries in addition to unigram formatting
		}
	}
}