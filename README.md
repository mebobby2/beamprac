# Beam Practice

## Development
Create virtual env
```
python3 -m venv /Users/BobbyLei/Desktop/learn/beamprac/venv
```

Activate virtual env
```
. /Users/BobbyLei/Desktop/learn/beamprac/venv/bin/activate
```

Update Pip
```
/Users/BobbyLei/Desktop/learn/beamprac/venv/bin/python3 -m pip install --upgrade pip
```

Run code
```
python wordcount.py --output /Users/BobbyLei/Desktop/learn/beamprac/data/wordcount_minimal_results.txt

or
gcloud auth application-default login
python taxi.py --input_topic projects/pubsub-public-data/topics/taxirides-realtime

or
python user_score.py --output /Users/BobbyLei/Desktop/learn/beamprac/data/user_score.txt

or
python hourly_team_score.py --output /Users/BobbyLei/Desktop/learn/beamprac/data/hourly_team_score --start_min 2015-11-16-16-00 --stop_min 2015-11-20-00-00 --window_duration 60

or
python leader_board.py --output /Users/BobbyLei/Desktop/learn/beamprac/data/leader_board --team_window_duration 60 --allowed_lateness 120 --topic
```

Run Unit Tests (from root dir)
```
python -m unittest
```

Deactivate
```
deactivate
```

## Install
(After activating the virtual env above)

Apache Beam
```
pip install apache-beam
```

Google Cloud Platform
Required for: Google Cloud Dataflow Runner, GCS IO, Datastore IO, BigQuery IO
```
pip install 'apache-beam[gcp]'
```

Tests
Required for developing on beam and running unittests
```
pip install 'apache-beam[test]'
```

Docs
Generating API documentation using Sphinx
```
pip install 'apache-beam[docs]'
```

## Upto
https://beam.apache.org/get-started/mobile-gaming-example/

GameStats: Abuse Detection and Usage Analysis

Before that: run the leaderboard pipeline.

Use https://stackoverflow.com/questions/56269102/whats-the-commands-to-run-the-apache-beam-mobile-gaming-tutorials to run injector to publish data to our own pubsub topic
