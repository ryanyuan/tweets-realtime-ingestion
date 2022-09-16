#!/usr/bin/env python3
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


PUBSUB_SUBSCRIPTION = "projects/eternal-mark-359107/subscriptions/tweets-sub"
BIGQUERY_TABLE = "eternal-mark-359107:demo.tweets"


class FormatDoFn(beam.DoFn):
    def process(self, element):
        tweet = json.loads(element.decode("utf-8"))
        yield {"word": tweet["text"], "ts": tweet["created_at"]}


def run():
    with beam.Pipeline(options=PipelineOptions()) as p:
        (
            p
            | "Read From PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=PUBSUB_SUBSCRIPTION
            ).with_output_types(bytes)
            | "Convert to Row" >> beam.ParDo(FormatDoFn())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema="word:STRING, ts:TIMESTAMP",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()
