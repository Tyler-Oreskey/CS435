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
    public void testParseYear() {
        assertEquals("2000", book.getBookYear(), "Year should be '2000'");
    }
}
