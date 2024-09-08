package ngram;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Book {
	private String headerText, bodyText, author, year;
	private int ngramCount;

	public Book(String rawText, int ngramCount) {
		this.ngramCount = ngramCount;

		String[] parts = rawText.split("\\*\\*\\*START OF THIS PROJECT GUTENBERG EBOOK.*\\*\\*\\*", 2);

		this.headerText = parts[0].trim();
		this.bodyText = parts[1].trim();
		this.author = parseAuthor(headerText);
		this.year = parseYear(headerText);
	}

	private String parseAuthor(String headerText) {
		// Author:\s: Matches "Author:" followed by any whitespace
		// .* Matches any character (except newline) zero or more times.
		// \b(\w+)\b: Matches the last word in the line.
		Pattern authorPattern = Pattern.compile("Author:\\s.*\\b(\\w+)\\b");
		Matcher authorMatcher = authorPattern.matcher(headerText);

		if (authorMatcher.find()) {
			return authorMatcher.group(1);
		}

		return "Unknown";
	}

	private String parseYear(String headerText) {
		Pattern yearPattern = Pattern.compile("Release Date:\\s.*\\b(\\w+)\\b");
		Matcher yearMatcher = yearPattern.matcher(headerText);

		if (yearMatcher.find()) {
			return yearMatcher.group(1);
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
		return "Unknown";
	}
}