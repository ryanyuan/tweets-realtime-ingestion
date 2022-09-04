# tweets-realtime-ingestion
A realtime data pipeline to ingestion tweets from Pub/Sub to BigQuery using Apache Beam Python SDK

## Prerequisite
```
$ pip3 install -r ./beam_pipeline/requirements.txt
```


## Execute pipeline using DirectRunner
```
$ python3 ./beam_pipeline/tweets_streaming.py --streaming --runner DirectRunner
```


## Execute pipeline using DataflowRunner

- project: your Google Cloud project ID.
- region: the regional endpoint for your Dataflow job.
- temp_location: a Cloud Storage path for Dataflow to stage temporary job files created during the execution of the pipeline.

```
$ python3 beam_pipeline/tweets_streaming.py --streaming --runner DataflowRunner --job_name tweets-df-`date "+%Y%m%d-%H%M%S"` --project {project} --region {region} --temp_location {temp_location}
```

## To spin up Twitter to Pub/Sub service

1. Conigure the env variables in ./twitter-to-pubsub/config.env
2. execute the commands below
```
$ pip3 install -r ./twitter-to-pubsub/requirements.txt
$ python3 ./twitter-to-pubsub/twitter-to-pubsub.py
```


