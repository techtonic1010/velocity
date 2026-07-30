package com.velocity.recommendation.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
// Used for the Bloom filter check in step 5 — 
// BloomFilterReadService.mightContain(userId, entityId).

// To check "has this user probably seen this article," 
// recommendation-service needs to hash (userId, entityId)
//  into the same 7 bit-positions that entity-interaction-service 
// set when writing that Bloom filter — so it must use the exact 
// same MurmurHash3 implementation, or the bit positions wouldn't 
// match and the check would be meaningless. Since there's no shared 
// library between services, MurmurHash3.java is duplicated verbatim
//  into recommendation-service, read-only (GETBIT only, never SETBIT).

// Not used for LIKE/DISLIKE — that stays exclusively in entity-interaction-service's 
// classify(), as we just settled.


class MurmurHash3Test {

    @Test
    void sameInputAndSeedAlwaysProduceSameHash() {
        byte[] data = "U131|N12345".getBytes(StandardCharsets.UTF_8);

        int first = MurmurHash3.hash32(data, 0);
        int second = MurmurHash3.hash32(data, 0);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void differentSeedsProduceDifferentHashesForSameInput() {
        byte[] data = "U131|N12345".getBytes(StandardCharsets.UTF_8);

        int seed0 = MurmurHash3.hash32(data, 0);
        int seed1 = MurmurHash3.hash32(data, 1);

        assertThat(seed0).isNotEqualTo(seed1);
    }

    @Test
    void emptyInputWithZeroSeedHashesToZero() {
        // Derived directly from the algorithm: with no blocks, no tail bytes, and length 0,
        // hash stays 0 through every mixing step, so finalize(0) == 0.
        assertThat(MurmurHash3.hash32(new byte[0], 0)).isZero();
    }

    @Test
    void handlesTailLengthsOfOneTwoAndThreeBytesWithoutError() {
        for (int length = 1; length <= 3; length++) {
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = (byte) (i + 1);
            }
            int hash = MurmurHash3.hash32(data, 0);
            int hashAgain = MurmurHash3.hash32(data, 0);
            assertThat(hash).isEqualTo(hashAgain);
        }
    }

    @Test
    void changingOneByteChangesTheHash() {
        byte[] data = "N12345".getBytes(StandardCharsets.UTF_8);
        byte[] mutated = "N12346".getBytes(StandardCharsets.UTF_8);

        assertThat(MurmurHash3.hash32(data, 0)).isNotEqualTo(MurmurHash3.hash32(mutated, 0));
    }
}
