package com.velocity.entityupload.parser;

import com.velocity.entityupload.model.NewsArticle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.stream.Stream;

/**
 * Parses MIND's news.tsv: tab-separated, no header row.
 * Columns (per PROJECT_SPEC.md #0): News ID, Category, SubCategory, Title, Abstract,
 * URL, Title Entities, Abstract Entities. Only the first 5 are needed for embeddings;
 * parsing tolerates rows shorter than 8 columns since trailing columns can be truncated
 * in some MIND mirrors.
 */
public final class MindNewsTsvParser {
    
    private static final int MIN_REQUIRED_COLUMNS = 4; // News ID, Category, SubCategory, Title
    private static final int ABSTRACT_COLUMN_INDEX = 4;
// final class with a private constructor, all-static methods — 
// the standard Java idiom for a stateless utility class

// final class + private constructor + static methods
// public final class MindNewsTsvParser {
//     private MindNewsTsvParser() {}
// }
// Prevents creating objects (new MindNewsTsvParser()).
// Prevents inheritance.
// All methods are static because parsing doesn't need any object state.

// ➡️ Stateless utility class.

    private MindNewsTsvParser() {
    }

    public static Stream<NewsArticle> parse(Reader reader) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(reader);
        return bufferedReader.lines()
                .filter(line -> !line.isBlank())
                .map(MindNewsTsvParser::parseLine);
    }
// With Stream:


// Read one row
//       ↓
// Process it
//       ↓
// Read next row

// Memory stays low and downstream code can batch rows lazily.

// ➡️ Lazy processing and memory efficiency.

// Category, SubCategory, and Title are ever load-bearing for embeddings;
//  URL and the two Entities columns are never read at all by this milestone,
//   and Abstract is explicitly optional

    private static NewsArticle parseLine(String line) {
        String[] columns = line.split("\t", -1);
        if (columns.length < MIN_REQUIRED_COLUMNS) {
            throw new IllegalArgumentException(
                    "news.tsv row has fewer than " + MIN_REQUIRED_COLUMNS + " columns: " + line);
        }

        String newsId = columns[0];
        String category = columns[1];
        String subcategory = columns[2];
        String title = columns[3];
        String abstractText = columns.length > ABSTRACT_COLUMN_INDEX
                ? columns[ABSTRACT_COLUMN_INDEX]
                : null;

        return new NewsArticle(
                newsId,
                category,
                subcategory,
                title,
                (abstractText == null || abstractText.isBlank()) ? null : abstractText);
    }
}

// 2. Takes a Reader, not File/Path
// parse(Reader reader)

// Instead of:

// parse(File file)

// This separates responsibilities:

// Parser → parses text.
// IngestionService → opens/closes files.

// ➡️ Loose coupling and easy testing.



// TSV File
//     │
//     ▼
// MindNewsTsvParser
//     │
//     ▼
// NewsArticle
//     │
//     ▼
// IngestionService
//     │
//     ├──────────────► EmbeddingCreatorClient
//     │                      │
//     │                      ▼
//     │              HTTP POST /embed
//     │                      │
//     │                      ▼
//     │          Embedding Creator (Python FastAPI)
//     │                      │
//     │              Returns embedding vector
//     │                      ▼
//     └────────────── Receives response
//                            │
//                            ▼
//                   Save to PostgreSQL