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

--------Things Did Start Here----------

gcloud iam service-accounts create bobby-play-sa --description="play" --display-name="Bobby Play"

gcloud iam service-accounts keys create ~/Downloads/bobby-play-sa.txt --iam-account bobby-play-sa@harmoney-core-platform-dev.iam.gserviceaccount.com

cat ~/Downloads/bobby-play-sa.txt
{
  "type": "service_account",
  "project_id": "harmoney-core-platform-dev",
  "private_key_id": "08967157fbebc1b9da8b62e90ac469dc23ad0cd6",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCYo0QEVmFsAFGx\nIWje+9OfQEMkHmWbb/8EVda7mvNQi+1vyZDyDzZe8XeD6Y3pOqLwwQk0Ll6M3e6I\nJVB4TwgBTT+lcF4yl+LK0sudPsgeMUZRV9bmloATPzu9eoQl3VXuRfxtQAPEgnsb\nSaHDla5Nk5qzJnj6w9Hj3RlfyzUS1FGnByu6p0K91ZNdyj9rBqKM+UT3GRgDbeOG\nXSrhtSDJyVZFADSsYo7jCez/VUspxFkPdjZf0c3ykBpXkuApG5RIQGYFtLif20EE\nKteRBor4wVko5PP1J10ntJtTUI1+ASH3Bjwiakbw8eFmUeklTrTLPvoIlew0Zkiv\nqOqQhe+1AgMBAAECggEACwkY0JB3PDH8tggCl0aROw0QHaMl+Po8r5FONGOkTlFD\nkia6WEnhxjUCjVK8PB3NbIckVXW5x8j+5zB9dClQnJl7bchlQxaoor3hU0XX/ZjV\n6uklJChC68FRXiZmuf8Z6J9+O97aNuGxKVymOagPgVPX5jZ0njq8qNMSM9MTDACZ\npqAX/Smqq51sOU3zPMeAwThdOCtJxjG/GbWOE6yBnV8xnUSlP6iOi5bKbc1M+QUB\ni8rvjd4pXf9vqtXwDb3l7YYrS5wFBSgJL5SEUV8j57wX+kwpE5joGU23U+Hl5Urk\nGgGI2Kl7+FzJIaA+NwKogxfAqI2qLtMMty7k4UzMOQKBgQDO6FrSjKc4t+lv+ZK1\ng4HIPReh9muhcPfOiNtZz8dAv98gksB3ojAJiYR1/dpnbDcBMZgn6eQBb5qLhkPv\nSC8oKD28h05uL96i9d6DvRGqO5xMLNJSmpVmx7hUN3+q0gLz+ORPmIFOlZbqp0cy\nodAKGYAYrKDifW773v0GiC7h7QKBgQC82onmKpn5nDzkcxsantnuoM861YkXdMIO\nlx1qnq6iAdkVYDhIF6ynkZyWijMxTtJ0m21OuXrQU1EZnYetPScPnn/GShHPdZbW\nIYxev2oeN2XD51T4bwHfIaIY6LhNDpRGt1yKDD/JX7s3O+GXqctU84LJhCMHxTCf\nPIGVRpWr6QKBgFI66g2PtM3H2dCUwYe+EhBAp2nJA5GP0gyVO4gAGdJT7xuE91UU\ncj74FlrGXsyHp3yj9zK1s9YWQrd8zvcEGym8hHyu0a7c+4CMcrispZ6gPkfP8G6/\n3OySJ2HhBK4g9Od9XU987rcKoIX1oUgwIvxAkjY8NVyOOVOElc7IxMkpAoGBAIoV\nxMStm53+Viu32jaRtcmcfUmIfs/OyGGArrqZwhik/0R48U4NOWngtzz/WcUQ3CL0\nzmzEg/81HFR9cYoC77+k5cpnuDQXx61UJF3W2dG0Mc2XJPLtv9GVtv89khyNr28x\nfRFXOYCVZb9SInYRaaH410VfP3nb/dxAkoNCdAeZAoGAdxmhAzxCgxe/0DRTQ09e\nwyEszuWHz7UXu+kWWgdJixuPhJR7U4td4EUwRJSR6aGMISynR2xgXbpf0cKUEY+n\nhT5KwdyHMbP/d/jXSAEvK3117EIANcLePGfDjjQRWsWglpIv+q7PLloY301OUuP+\nEuzUZRU8aQTu1er4nbAhAqk=\n-----END PRIVATE KEY-----\n",
  "client_email": "bobby-play-sa@harmoney-core-platform-dev.iam.gserviceaccount.com",
  "client_id": "110931794630374314942",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bobby-play-sa%40harmoney-core-platform-dev.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

gcloud pubsub topics create game
# Created topic [projects/harmoney-core-platform-dev/topics/game].

brew install maven
git clone git@github.com:apache/beam.git
cd beam

mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=${RELEASE_VER} \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false \
      -DarchetypeCatalog=internal

cd word-count-beam

export GOOGLE_APPLICATION_CREDENTIALS=~/Downloads/bobby-play-sa.txt

mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector -Dexec.args="harmoney-core-platform-dev game none"

Threw an error message:
Starting Injector
[+(BananaEchidna, num members: 14, starting at: 1685710307490, expires in: 28, robot: null)]
[+(AppleGreenPossum, num members: 18, starting at: 1685710307490, expires in: 36, robot: null)]
[+(FuchsiaDingo, num members: 10, starting at: 1685710307490, expires in: 22, robot: Robot-10)]
[+(AppleGreenMarmot, num members: 7, starting at: 1685710307490, expires in: 39, robot: null)]
[+(ArmyGreenKookaburra, num members: 15, starting at: 1685710307490, expires in: 21, robot: null)]
[+(AsparagusCockatoo, num members: 7, starting at: 1685710307490, expires in: 32, robot: null)]
[+(AliceBlueWallaby, num members: 12, starting at: 1685710307490, expires in: 20, robot: null)]
[+(AntiqueBrassWallaby, num members: 13, starting at: 1685710307490, expires in: 28, robot: Robot-6)]
[+(MagentaWombat, num members: 7, starting at: 1685710307490, expires in: 22, robot: Robot-12)]
[+(AuburnBandicoot, num members: 11, starting at: 1685710307491, expires in: 25, robot: Robot-18)]
[+(BattleshipGreyWombat, num members: 11, starting at: 1685710307491, expires in: 25, robot: null)]
[+(AntiqueBrassKookaburra, num members: 19, starting at: 1685710307491, expires in: 34, robot: null)]
[+(AlmondAntechinus, num members: 14, starting at: 1685710307491, expires in: 32, robot: null)]
[+(FuchsiaPossum, num members: 6, starting at: 1685710307491, expires in: 26, robot: null)]
[+(AquaKoala, num members: 6, starting at: 1685710307491, expires in: 32, robot: null)]
DELAY(323512, 1)
{timestamp_ms=1685709983000}
late data for: user1_MagentaWombat,MagentaWombat,6,1685709983000,2023-06-02 05:51:47.495
.com.google.api.client.googleapis.json.GoogleJsonResponseException: 403 Forbidden
{
  "code" : 403,
  "errors" : [ {
    "domain" : "global",
    "message" : "User not authorized to perform this action.",
    "reason" : "forbidden"
  } ],
  "message" : "User not authorized to perform this action.",
  "status" : "PERMISSION_DENIED"
}

Tried giving the SA permission, but my gcloud account has not permission to assign permissions
gcloud alpha pubsub topics add-iam-policy-binding game --member="serviceAccount:bobby-play-sa@harmoney-core-platform-dev.iam.gserviceaccount.com" --role='roles/pubsub.editor'

Tried to run injector with my gcloud account
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector -Dexec.args="harmoney-core-platform-dev game none"
but got error:
***Warning! You are not using service account credentials to authenticate.
You need to use service account credentials for this example,
since user-level credentials do not have enough pubsub quota,
and so you will run out of PubSub quota very quickly.
See https://developers.google.com/identity/protocols/application-default-credentials.
