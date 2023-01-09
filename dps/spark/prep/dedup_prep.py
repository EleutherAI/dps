from typing import List

import binascii
import numpy as np


MERSENNE_PRIME = (1 << 61) - 1
MAX_HASH = (1 << 32) - 1
HASH_RANGE = 1 << 32


def shingle_word(text: str, n_gram: int = 15, char_level: bool = False) -> List[str]:
    res = []
    text_words = text.split() if not char_level else text

    for i in range(len(text_words)):
        shingle = text_words[i : i + n_gram]

        if len(shingle) == n_gram:
            res.append("_".join(shingle).encode("utf-8"))

    return res


def generate_minhash(shingles: List, num_perm: int = 64, seed: int = 1) -> np.array:
    def hashfunc(b: bytes) -> bytes:
        return binascii.crc32(b) & MAX_HASH

    hashvalues = np.ones(num_perm, dtype=np.uint64) * MAX_HASH

    generator = np.random.RandomState(seed)
    permutations = np.array(
        [
            (
                generator.randint(1, MERSENNE_PRIME, dtype=np.uint64),
                generator.randint(0, MERSENNE_PRIME, dtype=np.uint64),
            )
            for _ in range(num_perm)
        ],
        dtype=np.uint64,
    ).T

    for shingle in shingles:
        hv = hashfunc(shingle)
        a, b = permutations
        phv = np.bitwise_and((a * hv + b) % MERSENNE_PRIME, np.uint64(MAX_HASH))
        hashvalues = np.minimum(phv, hashvalues)

    return hashvalues


def jaccard_by_hashvalues(src_hashvalues, tgt_hashvalues) -> float:
    if len(src_hashvalues) != len(tgt_hashvalues):
        raise ValueError()

    return np.float(np.count_nonzero(src_hashvalues == tgt_hashvalues)) / np.float(
        len(src_hashvalues)
    )
