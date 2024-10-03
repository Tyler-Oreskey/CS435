package ngram;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Book {
	private String headerText, bodyText, author, year;
	private int ngramCount;

	public Book(String rawText, int ngramCount) {
		this.ngramCount = ngramCount;

		String regex = "\\*\\*\\* [^*]+ \\*\\*\\*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(rawText);

        if (matcher.find()) {
            String[] parts = rawText.split(regex, 2);

			if (parts[0].isEmpty()|| parts[1].isEmpty()) {
				this.headerText = "Unknown";
				this.bodyText = "Unknown";
				this.author = "Unknown";
				this.year = "Unknown";
			}
			else {
				this.headerText = parts[0].trim();
				this.bodyText = formatBook(parts[1]);
				this.author = parseAuthor(headerText);
				this.year = parseYear(headerText);
			}
        } else {
            this.headerText = "Unknown";
            this.bodyText = "Unknown";
            this.author = "Unknown";
            this.year = "Unknown";
        }
	}

	private String parseAuthor(String headerText) {
		Pattern authorPattern = Pattern.compile("Author:\\s.*\\b(\\w+)\\b");
		Matcher authorMatcher = authorPattern.matcher(headerText);

		if (authorMatcher.find()) {
			return authorMatcher.group(1);
		}

		return "Unknown";
	}

	private String parseYear(String headerText) {
		Pattern yearPattern = Pattern.compile("Release Date:\\s*.*?(\\b\\d{4}\\b)");
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
		bookText = bookText.toLowerCase();

		if (ngramCount < 2) {
			return removePunctuation(bookText);
		} else {
			String[] sentences = bookText.split("[.!?]");
			StringBuilder result = new StringBuilder();

			for (String sentence : sentences) {
				String formattedSentence = removePunctuation(sentence);

				if (!formattedSentence.isEmpty()) {
					result.append("_START_ ")
						  .append(formattedSentence)
						  .append(" _END_ ");
				}
			}

			return result.toString().trim();
		}
	}

	private String removePunctuation(String text) {
			// Remove all punctuation except hyphens
			String punctuationRemovedText = text.replaceAll("[\\p{Punct}&&[^-]]+", "");

			// Remove all stand alone hyphens
			String hyphensAdjustedText = punctuationRemovedText.replaceAll("(?<!\\w)-|-(?!\\w)", "");

			// Change all occurrences of two or more whitespaces with a single whitespace.
			String result = hyphensAdjustedText.replaceAll("\\s{2,}", " ").trim();
			return result;
	}
}