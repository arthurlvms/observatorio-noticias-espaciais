"""Índices binários equivalentes. Cada termo contribui com IDF uma vez."""
import math
import re
import statistics
import struct
from collections import defaultdict
from time import perf_counter_ns

from src.pipeline import connect


def tokens(text):
    return set(re.findall(r"[^\W_]+", text.casefold()))


def pack_gaps(ids):
    out = bytearray()
    previous = 0
    for doc in ids:
        gap = doc - previous
        previous = doc
        while gap >= 128:
            out.append(gap % 128)
            gap //= 128
        out.append(gap + 128)
    return bytes(out)


def unpack_gaps(blob):
    doc = gap = shift = 0
    for byte in blob:
        gap += (byte & 127) << shift
        if byte & 128:
            doc += gap
            yield doc
            gap = shift = 0
        else:
            shift += 7


class Index:
    def __init__(self, rows):
        postings = defaultdict(list)
        self.docs = {}
        for doc, title, summary, url in rows:
            self.docs[doc] = (title, summary, url)
            for term in tokens(title + " " + summary):
                postings[term].append(doc)
        self.fixed = {}
        self.packed = {}
        self.idf = {}
        for term, ids in postings.items():
            ids.sort()
            self.fixed[term] = struct.pack(f"<{len(ids)}I", *ids)
            self.packed[term] = pack_gaps(ids)
            self.idf[term] = 1 + math.log((1 + len(self.docs)) / (1 + len(ids)))
        self.sizes = [sum(map(len, idx.values())) for idx in (self.fixed, self.packed)]

    def search(self, query, packed, k=10):
        scores = defaultdict(float)
        for term in sorted(tokens(query) & self.idf.keys()):
            if packed:
                ids = unpack_gaps(self.packed[term])
            else:
                ids = (doc for doc, in struct.iter_unpack("<I", self.fixed[term]))
            for doc in ids:
                scores[doc] += self.idf[term]
        return sorted(scores.items(), key=lambda x: (-x[1], x[0]))[:k]

    def benchmark(self, query, rounds=40):
        expected = self.search(query, False)
        assert expected == self.search(query, True)
        samples = [[], []]
        for i in range(rounds):
            for mode in (i % 2, 1 - i % 2):
                start = perf_counter_ns()
                result = self.search(query, mode)
                samples[mode].append((perf_counter_ns() - start) / 1e6)
                if result != expected:
                    raise ValueError("Divergência no ranking")
        before, after = map(statistics.mean, samples)
        return before, after, 100 * (1 - after / before), before / after


def read_index(db):
    return Index(db.execute("""SELECT sk_artigo,titulo,resumo,url
                               FROM fato_publicacao JOIN dim_artigo USING(sk_artigo)"""))


if __name__ == "__main__":
    with connect() as conn:
        index = read_index(conn)
    print("Documentos, termos e bytes:", len(index.docs), len(index.fixed), index.sizes)
    for q in ("launch", "space station", "lunar lander", "satellite spectrum"):
        print(q, *[round(x, 5) for x in index.benchmark(q, 200)])
