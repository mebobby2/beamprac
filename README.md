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

LeaderBoard: Streaming Processing with Real-Time Game Data
