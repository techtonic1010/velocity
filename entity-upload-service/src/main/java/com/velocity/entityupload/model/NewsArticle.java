package com.velocity.entityupload.model;

public record NewsArticle(
        String newsId,
        String category,
        String subcategory,
        String title,
        String abstractText) {

            //return the final text for embedding , which is the title and 
            // abstract concatenated together
    public String embeddingText() {
        String abstractPart = abstractText == null ? "" : abstractText;
        return (title + " " + abstractPart).trim();
    }
}

// =>2. Why String instead of Optional<String>?

// No conversion is needed.

// If you used Optional<String>, you'd have to wrap every value:

// Optional.ofNullable(dbValue)

// and unwrap it everywhere:

// article.abstractText().orElse("")