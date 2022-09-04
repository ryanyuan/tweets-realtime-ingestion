#!/usr/bin/env python3
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import translate_v2 as translate


PUBSUB_SUBSCRIPTION = "projects/eternal-mark-359107/subscriptions/tweets-sub"
BIGQUERY_TABLE_ENG = "eternal-mark-359107:pygeekle22.tweets_english"
BIGQUERY_TABLE_OTHERS = "eternal-mark-359107:pygeekle22.tweets_others"

translate_client = translate.Client()


class DetectLanguageDoFn(beam.DoFn):
    def process(self, element):
        tweet = json.loads(element.decode("utf-8"))
        tweet_text = tweet["text"]
        tweet_created_at = tweet["created_at"]
        tweet_language = translate_client.detect_language(tweet_text)["language"]

        if tweet_language == "en":
            yield beam.pvalue.TaggedOutput(
                "eng", {"word": tweet_text, "ts": tweet_created_at}
            )
        else:
            yield {"word": tweet_text, "ts": tweet_created_at}


def run():

    with beam.Pipeline(options=PipelineOptions()) as p:
        all_tweets = (
            p
            | "Read From PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=PUBSUB_SUBSCRIPTION
            ).with_output_types(bytes)
            | "Detect Lanaguage"
            >> beam.ParDo(DetectLanguageDoFn()).with_outputs("eng", main="others")
        )

        tweet_others = all_tweets.others
        tweet_eng = all_tweets.eng

        (
            tweet_others
            | "Write Other Tweets to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE_OTHERS,
                schema="word:STRING, language:STRING, ts:TIMESTAMP",
            )
        )

        (
            tweet_eng
            | "Write English Tweets to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE_ENG,
                schema="word:STRING, language:STRING, ts:TIMESTAMP",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    run()
