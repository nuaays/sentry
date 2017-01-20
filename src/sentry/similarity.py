from __future__ import absolute_import

import itertools
import math
import random
from collections import Counter


def advance(n, iterator):
    """Advances an iterator n places."""
    next(itertools.islice(iterator, n, n), None)
    return iterator


def to_shingles(n, tokens):
    """Shingle a token stream into n-grams."""
    return itertools.izip(
        *map(
            lambda (i, iterator): advance(i, iterator),
            enumerate(itertools.tee(tokens, n)),
        )
    )


def scale_to_total(value):
    total = float(sum(value.values()))
    return {k: (v / total) for k, v in value.items()}


def get_distance(target, other):
    return math.sqrt(
        sum(
            (target.get(k, 0) - other.get(k, 0)) ** 2
            for k in set(target) | set(other)
        )
    )


def get_similarity(target, other):
    assert len(target) == len(other)
    return 1 - sum(
        map(
            lambda (left, right): get_distance(
                left,
                right,
            ) / math.sqrt(2),
            zip(target, other)
        )
    ) / len(target)


def format_buckets(bucket):
    # TODO: Make this better!
    return ','.join(map('{}'.format, bucket))


class Feature(object):
    def record(self, label, scope, key, value):
        raise NotImplementedError


class MinHashFeature(Feature):
    def __init__(self, cluster, rows, bands, buckets):
        self.namespace = 'sim'

        self.cluster = cluster
        self.rows = rows

        generator = random.Random(0)

        def shuffle(value):
            generator.shuffle(value)
            return value

        self.bands = [
            [shuffle(range(rows)) for _ in xrange(buckets)]
            for _ in xrange(bands)
        ]

    def get_similar(self, scope, key):
        data = {}  # TODO(tkaemming): Name this something that is not 100% unhelpful

        def fetch_data(keys):
            with self.cluster.map() as client:
                responses = {
                    key: map(
                        lambda band: client.zrange(
                            '{}:{}:1:{}:{}'.format(self.namespace, scope, band, key),
                            0,
                            -1,
                            desc=True,
                            withscores=True,
                        ),
                        range(len(self.bands)),
                    ) for key in keys if key not in data
                }

            for key, promises in responses.items():
                data[key] = map(
                    lambda promise: scale_to_total(
                        dict(promise.value)
                    ),
                    promises,
                )

        def fetch_candidates(data):
            with self.cluster.map() as client:
                responses = map(
                    lambda (band, buckets): map(
                        lambda bucket: client.smembers(
                            '{}:{}:0:{}:{}'.format(self.namespace, scope, band, bucket)
                        ),
                        buckets,
                    ),
                    enumerate(data),
                )

            return map(
                lambda band: reduce(
                    lambda values, promise: values | promise.value,
                    band,
                    set(),
                ),
                responses,
            )

        fetch_data([key])

        candidates = Counter()
        for keys in fetch_candidates(data[key]):
            candidates.update(keys)

        fetch_data(candidates.keys())

        return sorted(
            ((
                candidate,
                get_similarity(
                    data[key],
                    data[candidate],
                ),
            ) for candidate in data),
            key=lambda (key, similarity): similarity,
            reverse=True,
        )

    def get_signature(self, value):
        columns = set(hash(token) % self.rows for token in value)
        return map(
            lambda band: map(
                lambda permutation: next(i for i, a in enumerate(permutation) if a in columns),
                band,
            ),
            self.bands,
        )

    def record(self, scope, key, value):
        with self.cluster.map() as client:
            for band, buckets in enumerate(self.get_signature(value)):
                client.sadd(
                    '{}:{}:0:{}:{}'.format(self.namespace, scope, band, format_buckets(buckets)),
                    key,
                )
                client.zincrby(
                    '{}:{}:1:{}:{}'.format(self.namespace, scope, band, key),
                    format_buckets(buckets),
                    1,
                )
