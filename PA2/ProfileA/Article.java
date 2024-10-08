package ProfileA;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Article {
    private String docID;
    private String bodyText;
	private static final Pattern NON_ALPHANUMERIC_PATTERN = Pattern.compile("[^a-zA-Z0-9\\s]");

	public Article(String rawText) {
        String[] parts = rawText.split("<====>");
        if (parts.length == 3) {
            this.docID = parts[1].trim();
            this.bodyText = removeNonAlphanumeric(parts[2].trim()).toLowerCase();
        } else {
            this.docID = "Unknown";
            this.bodyText = "";
        }
	}

	public String getDocID() {
        return this.docID;
    }

	public String getBody() {
		return bodyText;
	}

    private String removeNonAlphanumeric(String text) {
        // Remove non-alphanumeric characters
        return NON_ALPHANUMERIC_PATTERN.matcher(text).replaceAll(" ").replaceAll("\\s{2,}", " ").trim();
    }
}