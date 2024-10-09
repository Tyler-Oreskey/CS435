package ProfileB;

public class Article {
    private String docID;
    private String bodyText;
    private static final String SENTENCE_DELIMITER = "\\.\\s";

    public Article(String rawText) {
        String[] parts = rawText.split("<====>");
        if (parts.length == 3) {
            this.docID = parts[1].trim();
            this.bodyText = parts[2].trim();
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

    public String[] getSentences() {
        return this.bodyText.split(SENTENCE_DELIMITER);
    }

    // Add a parameter for numSentences
    public String generateSummary(int numSentences) {
        String[] sentences = getSentences();
        StringBuilder summary = new StringBuilder();

        // Check if numSentences is less than or equal to the length of sentences
        for (int i = 0; i < Math.min(numSentences, sentences.length); i++) {
            summary.append(sentences[i].trim()).append(". ");
        }

        return summary.toString().trim();
    }
}
