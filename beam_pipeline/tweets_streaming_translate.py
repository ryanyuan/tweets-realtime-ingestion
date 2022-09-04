#!/usr/bin/env python3
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import translate_v2 as translate


PUBSUB_SUBSCRIPTION = "projects/eternal-mark-359107/subscriptions/tweets-sub"
BIGQUERY_TABLE = "eternal-mark-359107:pygeekle22.tweets_with_language"

translate_client = translate.Client()


class DetectLanguageDoFn(beam.DoFn):
    def process(self, element):
        tweet = json.loads(element.decode("utf-8"))
        tweet_text = tweet["text"]
        tweet_created_at = tweet["created_at"]
        tweet_language = translate_client.detect_language(tweet_text)["language"]

        yield {"word": tweet_text, "language": tweet_language, "ts": tweet_created_at}


def run():
    with beam.Pipeline(options=PipelineOptions()) as p:
        (
            p
            | "Read From Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=PUBSUB_SUBSCRIPTION
            ).with_output_types(bytes)
            | "Detect Lanaguage" >> beam.ParDo(DetectLanguageDoFn())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema="word:STRING, language:STRING, ts:TIMESTAMP",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()
