package com.velocity.entityupload.dto;

import java.util.List;

// EmbedResponse(String entityId, List<Double> vector) 
// — the one real decision here is List<Double> rather than double[]. Two reasons:


// Why List<Double>?

// Lists compare contents, not references.
// Why not double[]?

// Records automatically generate:

// equals()
// hashCode()
// toString()

// But arrays don't behave well with these methods.

public record EmbedResponse(String entityId, List<Double> vector) {
}

// What they are and why: these mirror the exact JSON contract main.
// py's Pydantic models define — {entityId, text} in, {entityId, vector} out. 
// The interesting part is what's not here: Python needed Field(alias="entityId") 
// because its internal convention is snake_case (entity_id) but the wire format is camelCase.
//  Java field names are already camelCase by convention, 
//  so Jackson (Spring Boot's default JSON library) serializes/deserializes entityId
//   correctly with zero annotations — the two languages' own naming conventions happen 
//   to line up with the wire format from opposite directions