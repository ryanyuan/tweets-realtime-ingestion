#!/usr/bin/env python

"""This script uses the Twitter Streaming API, via the tweepy library,
to pull tweets and publish them to a GCP Pub/Sub topic.
"""

import logging
import configparser

import tweepy
import json
from google.cloud import pubsub_v1


# Set your twitter and GCP configurations (e.g. credentials, Pub/Sub topic)
# in the 'config.env' manifest file.
CONFIG = configparser.RawConfigParser()
CONFIG.read("config.env")
TWITTER_BEARER_TOKEN = CONFIG["Twitter"]["bearer_token"]
GCP_PROJECT_NAME = CONFIG["GCP"]["project_name"]
GCP_TOPIC_NAME = CONFIG["GCP"]["topic_name"]

PUBLISHER_CLIENT = pubsub_v1.PublisherClient()
GCP_TOPIC_PATH = PUBLISHER_CLIENT.topic_path(GCP_PROJECT_NAME, GCP_TOPIC_NAME)


class TweetPrinter(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        try:
            if tweet.text:
                pubsub_msg = json.dumps(
                    {"text": tweet.text, "created_at": tweet.created_at}, default=str
                )
                PUBLISHER_CLIENT.publish(GCP_TOPIC_PATH, pubsub_msg.encode("utf-8"))
        except Exception as e:
            print(f"Unable to send to Pub/Sub, {e}")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s",
        level=logging.DEBUG,
    )

    streaming_client = TweetPrinter(TWITTER_BEARER_TOKEN)
    tweet_thread = streaming_client.sample(
        tweet_fields=["created_at", "public_metrics"]
    )
