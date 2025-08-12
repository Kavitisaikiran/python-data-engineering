import json
import os 
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime



def lambda_handler(event, context):
    client_id = os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    paylist_link = "https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE"
    paylist_url = paylist_link.split("/")[-1].split("?")[0]
    raw_data = sp.playlist_tracks(paylist_url)
    file_name = "raw_data" + str(datetime.now()) + ".json"
    client = boto3.client("s3")
    client.put_object(Bucket="saikiran-spotify-etl-project",
     Key="raw_data_file/not_processed/"+ file_name, Body=json.dumps(raw_data))
