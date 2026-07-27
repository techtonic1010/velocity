package com.velocity.entityinteraction.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

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
    void differentInputsProduceDifferentHashesForSameSeed() {
        int helloHash = MurmurHash3.hash32("hello".getBytes(StandardCharsets.UTF_8), 0);
        int worldHash = MurmurHash3.hash32("world".getBytes(StandardCharsets.UTF_8), 0);

        assertThat(helloHash).isNotEqualTo(worldHash);
    }

    @Test
    void emptyInputWithZeroSeedHashesToZero() {
        // Derived directly from the algorithm: with no blocks, no tail bytes, and length 0,
        // hash stays 0 through every mixing step, so finalize(0) == 0.
        assertThat(MurmurHash3.hash32(new byte[0], 0)).isZero();
    }

    @Test
    void handlesTailLengthsOfOneTwoAndThreeBytesWithoutError() {
        // Exercises every branch of the fall-through switch in the tail-mixing step (block size is 4).
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
