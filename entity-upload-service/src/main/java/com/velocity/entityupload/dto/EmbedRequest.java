package com.velocity.entityupload.dto;
// this is what gets sent as the JSON body of the POST /embed call
public record EmbedRequest(String entityId, String text) {
}

// Records auto-generate equals()/hashCode()/toString() based on their fields — 
// and arrays break that. double[] doesn't override equals() at all 
// (it's reference equality, inherited from Object), so two EmbedResponse records 
// holding the same numbers in a double[] would report as unequal, and toString() would
//  print something useless like [D@1a2b3c4 instead of the actual values.
//      List<Double> behaves the way you'd actually want in tests, logs, and debugging — 
//      this bit us nowhere yet, but it's exactly the kind of thing that's invisible until 
//      the one time you diff two responses in a test and can't figure out why they don't 
//      match.