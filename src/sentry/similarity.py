from __future__ import absolute_import

import functools
import itertools
import math
import random
from collections import Counter, defaultdict
from sentry.utils import redis
from sentry.utils.iterators import chunked


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


def format_bucket(bucket):
    # TODO: Make this better!
    return ','.join(map('{}'.format, bucket))


class Feature(object):
    def record(self, label, scope, key, value):
        raise NotImplementedError


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


class MinHashFeature(Feature):
    def __init__(self, cluster, tokenizer, rows, permutations, bands, namespace='sim'):
        self.cluster = cluster
        self.namespace = namespace

        self.tokenizer = tokenizer
        self.rows = rows

        generator = random.Random(0)

        def shuffle(value):
            generator.shuffle(value)
            return value

        assert permutations % bands == 0
        self.permutations = [shuffle(range(rows)) for _ in xrange(permutations)]
        self.band_size = permutations / bands

    def get_signatures_for_value(self, value):
        for stream in self.tokenizer(value):
            columns = set(hash(token) % self.rows for token in stream)
            try:
                yield map(
                    lambda p: next(i for i, a in enumerate(p) if a in columns),
                    self.permutations
                )
            except StopIteration:
                # TODO: Log this, or something...?
                pass

    def get_similar(self, label, scope, key):
        bands = range(len(self.permutations) / self.band_size)

        def fetch_data(client, key):
            return map(
                lambda band: client.zrange(
                    '{}:{}:{}:1:{}:{}'.format(
                        self.namespace,
                        label,
                        scope,
                        band,
                        key,
                    ),
                    0,
                    -1,
                    desc=True,
                    withscores=True,
                ),
                bands,
            )

        with self.cluster.map() as client:
            responses = fetch_data(client, key)

        values = [dict(r.value) for r in responses]

        responses = []
        with self.cluster.map() as client:
            for band, buckets in enumerate(values):
                responses.append([
                    client.smembers(
                        '{}:{}:{}:0:{}:{}'.format(
                            self.namespace,
                            label,
                            scope,
                            band,
                            bucket,
                        )
                    ) for bucket in buckets
                ])

        candidates = Counter()
        for promises in responses:
            candidates.update(
                reduce(
                    lambda values, promise: values | promise.value,
                    promises,
                    set(),
                )
            )

        n = float(len(self.permutations))
        candidates = map(
            lambda (item, count): (
                item,
                (count * self.band_size) / n,
            ),
            candidates.most_common(),
        )

        with self.cluster.map() as client:
            data = map(
                functools.partial(fetch_data, client),
                [item for item, count in candidates],
            )

        values = map(scale_to_total, values)

        results = []

        for (key, similarity), promises in zip(candidates, data):
            results.append((
                key,
                1 - sum([
                    get_distance(
                        value,
                        scale_to_total(
                            dict(promise.value)
                        ),
                    ) / 2.0
                    for value, promise in
                    zip(values, promises)
                ]) / float(len(self.permutations) / self.band_size),
            ))

        return sorted(
            results,
            key=lambda (k, v): v,
        )

    def record(self, label, scope, key, value):
        with self.cluster.map() as client:
            for signature in self.get_signatures_for_value(value):
                for band, bucket in enumerate(map(tuple, chunked(signature, self.band_size))):
                    client.sadd(
                        '{}:{}:{}:0:{}:{}'.format(
                            self.namespace,
                            label,
                            scope,
                            band,
                            format_bucket(bucket),
                        ),
                        key,
                    )

                    client.zincrby(
                        '{}:{}:{}:1:{}:{}'.format(
                            self.namespace,
                            label,
                            scope,
                            band,
                            key
                        ),
                        format_bucket(bucket),
                        1,
                    )


class FeatureManager(object):
    def __init__(self, features):
        self.features = features

    def record(self, scope, key, value):
        for label, (weight, feature) in self.features.items():
            feature.record(label, scope, key, value)

    def get_similar(self, scope, key):
        results = defaultdict(dict)
        for label, (weight, feature) in self.features.items():
            for k, distance in feature.get_similar(label, scope, key):
                results[k][label] = distance

        def score(value):
            return sum(value.get(label, 0) * weight for label, (weight, feature) in self.features.items())

        return sorted(
            [(id, score(features), features) for id, features in results.items()],
            key=lambda (id, score, features): score,
            reverse=True,
        )


def get_exceptions(event):
    return event.data.get(
        'sentry.interfaces.Exception',
        {'values': []}
    )['values']


def get_exception_message(exception):
    return exception['value'].split()  # XXX: This is obviously super naive.


def get_frames(exception):
    stacktrace = exception.get('stacktrace')
    if not stacktrace:
        return []
    return stacktrace['frames'] or []


def tokenize_frame(frame):
    return '{}.{}'.format(
        frame.get('module', '?'),
        frame.get('function', '?'),
    )


features = FeatureManager({
    'message': (0.3, MinHashFeature(
        redis.clusters.get('default'),
        lambda event: map(
            lambda exception: to_shingles(
                5,
                get_exception_message(exception),
            ),
            get_exceptions(event),
        ),
        0xFFFF, 16, 8,
    )),
    'frames': (0.7, MinHashFeature(
        redis.clusters.get('default'),
        lambda event: map(
            lambda exception: map(
                tokenize_frame,
                get_frames(exception),
            ),
            get_exceptions(event),
        ),
        0xFFFF, 16, 8,
    )),
})
