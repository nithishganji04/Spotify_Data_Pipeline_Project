import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
def lambda_handler(event, context):
    client_id=os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
    
    playlist='https://open.spotify.com/playlist/55R8yZjaAiUH3NWr5qvcyW'
    playlist_URI = playlist.split('/')[-1]
    data = sp.playlist_tracks(playlist_URI)
    print(data)
    
    client = boto3.client('s3')

    filename = "spotify_" +str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket = "spotifyetlnithish",
        Key = "raw-data/to-processed/" + filename,
        Body = json.dumps(data)
        )