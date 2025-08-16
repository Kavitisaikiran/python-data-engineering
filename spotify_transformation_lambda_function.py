import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
import spotipy


def album(raw_data):
    album_list = []
    for row in raw_data['items']:
        album_id = row["track"]["album"]["id"]
        album_name = row["track"]["album"]["name"]
        album_url = row["track"]["album"]["external_urls"]["spotify"]
        album_release_date = row["track"]["album"]["release_date"]
        album_track_duration = row["track"]["duration_ms"]
        album_elements = {
            'album_id': album_id,
            'album_name': album_name,
            'album_url': album_url,
            'album_release_date': album_release_date,
            'album_track_duration': album_track_duration
        }
        album_list.append(album_elements)
    return album_list


def artist(raw_data):
    artist_list = []
    for row in raw_data["items"]:
        for key, value in row.items():
            if key == "track":
                for artist in value["artists"]:
                    artist_dict = {
                        'artist_id': artist['id'],
                        'artist_name': artist["name"],
                        'external_url': artist['href']
                    }
                    artist_list.append(artist_dict)
    return artist_list


def songs(raw_data):
    song_list = []
    for row in raw_data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {
            'song_id': song_id,
            'song_name': song_name,
            'duration_ms': song_duration,
            'url': song_url,
            'popularity': song_popularity,
            'song_added': song_added,
            'album_id': album_id,
            'artist_id': artist_id
        }
        song_list.append(song_element)
    return song_list


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = "saikiran-spotify-etl-project"
    key = "raw_data_file/not_processed/"
    s3.list_objects(Bucket=bucket, Prefix=key)

    response = s3.list_objects(Bucket=bucket, Prefix=key)
    raw_data = []
    raw_data_keys = []
    if "Contents" not in response:
        print("No files found.")
        # print(json.dumps(response, indent=2))
    else:
        # Iterate only when there are objects
        for file in response["Contents"]:
            file_key = file["Key"]
            if file_key.endswith(".json"):
                resp = s3.get_object(Bucket=bucket, Key=file_key)
                data = resp["Body"].read()
                obj = json.loads(data)
                raw_data.append(obj)
                raw_data_keys

    for data in raw_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)

        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        song_df = pd.DataFrame.from_dict(song_list)
        song_df["song_added"] = pd.to_datetime(song_df["song_added"])
        album_df["album_release_date"] = pd.to_datetime(
            album_df["album_release_date"], format="mixed", errors="coerce"
        )

        song_key = "Transformed_data/song_data/" + str(datetime.now()) + ".csv"
        album_key = "Transformed_data/album_data/" + str(datetime.now()) + ".csv"
        artist_key = "Transformed_data/artist_data/" + str(datetime.now()) + ".csv"

        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=song_key, Body=song_content)

        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=album_key, Body=album_content)

        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=artist_key, Body=artist_content)

    # to work on resource level we need to .resource()
    s3_resource = boto3.resource('s3')
    for key in raw_data_keys:
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, bucket, "raw_data_file/processed/" + key.split("/")[-1])
        s3_resource.Object(bucket, key).delete()

