import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime



def lambda_handler(event, context):
    
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp_conn = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp_conn.user_playlists('spotify')
    
    playlist_link = "https://open.spotify.com/playlist/1KNl4AYfgZtOVm9KHkhPTF" #Top 50 global 2023
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp_conn.playlist_tracks(playlist_URI)   
    
    client = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket="spotify-etl-project-de",
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(spotify_data)
        )