package com.velocity.entityinteraction.util;

// Hand-rolled MurmurHash3 x86-32 (Austin Appleby's public-domain algorithm) — deliberately not an
// external library, since this project has no hashing dependency and both call sites (the
// LIKE/DISLIKE simulation and the Bloom filter) only need a fast, deterministic, well-distributed hash.
public final class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private MurmurHash3() {
    }

    public static int hash32(byte[] data, int seed) {
        int hash = seed;
        int length = data.length;
        int blockCount = length / 4;

        for (int i = 0; i < blockCount; i++) {
            int k = (data[i * 4] & 0xff)
                    | ((data[i * 4 + 1] & 0xff) << 8)
                    | ((data[i * 4 + 2] & 0xff) << 16)
                    | ((data[i * 4 + 3] & 0xff) << 24);
            hash = mixBlock(hash, k);
        }

        int tailStart = blockCount * 4;
        int tail = 0;
        switch (length - tailStart) {
            case 3:
                tail ^= (data[tailStart + 2] & 0xff) << 16;
            case 2:
                tail ^= (data[tailStart + 1] & 0xff) << 8;
            case 1:
                tail ^= (data[tailStart] & 0xff);
                tail *= C1;
                tail = Integer.rotateLeft(tail, 15);
                tail *= C2;
                hash ^= tail;
        }

        hash ^= length;
        return finalize(hash);
    }

    private static int mixBlock(int hash, int k) {
        k *= C1;
        k = Integer.rotateLeft(k, 15);
        k *= C2;
        hash ^= k;
        hash = Integer.rotateLeft(hash, 13);
        return hash * 5 + 0xe6546b64;
    }

    private static int finalize(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}
