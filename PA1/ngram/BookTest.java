package ngram;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BookTest {

    private Book book;

    @BeforeEach
    public void setUp() {
        String rawText = "Title: Example Book\n" +
                         "Author: John Doe\n" +
                         "Release Date: January 1, 2000\n" +
                         "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                         "This is the body of the book. It contains text and more text.";
        book = new Book(rawText, 2);
    }

    @Test
    public void testParseAuthorNoMiddleName() {
        assertEquals("Doe", book.getBookAuthor(), "Author should be 'Doe'");
    }

    @Test
    public void testParseAuthorSingleName() {
        String rawText = "Title: Example Book\n" +
                        "Author: John\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is the body of the book. It contains text and more text.";
        book = new Book(rawText, 2);
        assertEquals("John", book.getBookAuthor(), "Author should be 'John'");
    }

    @Test
    public void testParseAuthorMiddleName() {
        String rawText = "Title: Example Book\n" +
                        "Author: Edwin A. Abbot\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is the body of the book. It contains text and more text.";
        book = new Book(rawText, 2);
        assertEquals("Abbot", book.getBookAuthor(), "Author should be 'Abbot'");
    }

    @Test
    public void testUnigramFormatting() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is the body of the book. It contains text and more text.";

        book = new Book(rawText, 1);
        String formattedText = book.getBookBody();
        String expected = "this is the body of the book it contains text and more text";
        assertEquals(expected, formattedText, "Unigram text should be formatted correctly");
    }

    @Test
    public void testUnigramFormattingWithCompoundWords() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This-is the body of the-book. It contains text and more text.";

        book = new Book(rawText, 1);
        String formattedText = book.getBookBody();
        String expected = "this-is the body of the-book it contains text and more text";
        assertEquals(expected, formattedText, "Unigram text should be formatted correctly");
    }

    @Test
    public void testUnigramFormattingStandAloneHyphens() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is the body of the-book. It - contains text and more text. -";

        book = new Book(rawText, 1);
        String formattedText = book.getBookBody();
        String expected = "this is the body of the-book it contains text and more text";
        assertEquals(expected, formattedText, "Unigram text should be formatted correctly");
    }

    @Test
    public void testUnigramFormattingPrecededHyphens() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is the body of the-book. It -contains text and more text.";

        book = new Book(rawText, 1);
        String formattedText = book.getBookBody();
        String expected = "this is the body of the-book it contains text and more text";
        assertEquals(expected, formattedText, "Unigram text should be formatted correctly");
    }

    @Test
    public void testUnigramFormattingSucceededHyphens() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "- This is the body of the-book. It contains- text and more text.-";

        book = new Book(rawText, 1);
        String formattedText = book.getBookBody();
        String expected = "this is the body of the-book it contains text and more text";
        assertEquals(expected, formattedText, "Unigram text should be formatted correctly");
    }

    @Test
    public void testUnigramFormattingSucceededAndPreceededHyphens() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is the body of the-book. It -contains- text and more text.-";

        book = new Book(rawText, 1);
        String formattedText = book.getBookBody();
        String expected = "this is the body of the-book it contains text and more text";
        assertEquals(expected, formattedText, "Unigram text should be formatted correctly");
    }

    @Test
    public void testUnigramFormattingApostrophe() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is the body of you're book. It contains text and more text.";

        book = new Book(rawText, 1);
        String formattedText = book.getBookBody();
        String expected = "this is the body of youre book it contains text and more text";
        assertEquals(expected, formattedText, "Unigram text should be formatted correctly");
    }

    @Test
    public void testBigramFormattingSingleSentence() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is a test.";
        
        book = new Book(rawText, 2);  // ngramCount = 2 for bigram
        String formattedText = book.getBookBody();

        String expected = "_START_ this is a test _END_";
        assertEquals(expected, formattedText, "Bigram text should be formatted correctly with _START_/_END_ tokens");
    }

    @Test
    public void testBigramFormattingMultiSentence() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is a test. Why is this a test?";
        
        book = new Book(rawText, 2);  // ngramCount = 2 for bigram
        String formattedText = book.getBookBody();

        String expected = "_START_ this is a test _END_ _START_ why is this a test _END_";
        assertEquals(expected, formattedText, "Bigram text should be formatted correctly with _START_/_END_ tokens");
    }

    @Test
    public void testBigramFormattingMultiSentenceAndApostrophe() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "This is a test. You're a test.";
        
        book = new Book(rawText, 2);  // ngramCount = 2 for bigram
        String formattedText = book.getBookBody();

        String expected = "_START_ this is a test _END_ _START_ youre a test _END_";
        assertEquals(expected, formattedText, "Bigram text should be formatted correctly with _START_/_END_ tokens");
    }

    @Test
    public void testBigramFormattingMultiSentenceAndHyphens() {
        String rawText = "Title: Example Book\n" +
                        "Author: John Doe\n" +
                        "Release Date: January 1, 2000\n" +
                        "***START OF THIS PROJECT GUTENBERG EBOOK EXAMPLE BOOK***\n" +
                        "-This is a - test! You're-a test.-";
        
        book = new Book(rawText, 2);  // ngramCount = 2 for bigram
        String formattedText = book.getBookBody();

        String expected = "_START_ this is a test _END_ _START_ youre-a test _END_";
        assertEquals(expected, formattedText, "Bigram text should be formatted correctly with _START_/_END_ tokens");
    }
}
