package ProfileB;

public class Article {
    private String docID;
    private String bodyText;

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
        return getBody().split("\\.\\s");
    }
}
