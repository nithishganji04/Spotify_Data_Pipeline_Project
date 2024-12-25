import json
import boto3
import os
from io import StringIO
from datetime import datetime
import pandas as pd

def album(data):
    album_list=[]
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_uri= row['track']['album']['uri']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_elemnt = {'album_id' : album_id, 'album_name' : album_name, 'album_uri' : album_uri, 'album_release_date' : album_release_date,'album_total_tracks':album_total_tracks}
        album_list.append(album_elemnt)
    return album_list

def artist(data):
    artist_list=[]
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_id=artist['id']
                    artist_name=artist['name']
                    artist_uri=artist['uri']
                    artist_href=artist['href']
                    artist_list.append({'artist_id':artist_id,'artist_name':artist_name,'artist_uri':artist_uri,'artist_href':artist_href})
    return artist_list
    
def songs(data):
    songs_list=[]
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_href = row['track']['href']
        song_popularity = row['track']['popularity']
        song_uri = row['track']['uri']
        song_duration=row['track']['duration_ms']
        album_id=row['track']['album']['id']
        artist_id=row['track']['album']['artists'][0]['id']
        songs_list.append({'song_id':song_id,'song_name':song_name,'song_href':song_href,'song_popularity':song_popularity,'song_uri':song_uri,'song_duration':song_duration,'album_id':album_id,'artist_id':artist_id})
    
    return songs_list
    
    


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = "spotifyetlnithish"
    Key = "raw-data/to-processed/"

    spotify_data=[]
    spotify_keys=[]

    for file in s3.list_objects(Bucket=Bucket,Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object (Bucket=Bucket,Key =file_key)
            content =response['Body']
            jsonobject = json.loads(content.read())
            print(jsonobject)
            spotify_data.append(jsonobject)
            spotify_keys.append(file_key)
    

    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        songs_list = songs(data)
        

        album_df = pd.DataFrame.from_dict(album_list)
        album_df=album_df.drop_duplicates(subset=['album_id'])
        album_df['album_release_date']=pd.to_datetime(album_df['album_release_date'])

        artists_df=pd.DataFrame.from_dict(artist_list)
        artists_df=artists_df.drop_duplicates(subset=['artist_id'])

        songs_df=pd.DataFrame.from_dict(songs_list)
        songs_df=songs_df.drop_duplicates(subset=['song_id'])

        songs_key ="transformed-data/songs-data/"+str(datetime.now()) +".csv"

        song_buffer =StringIO()
        songs_df.to_csv(song_buffer,index=False)
        song_content =song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)

        album_key ="transformed-data/album-data/"+str(datetime.now()) +".csv"

        album_buffer =StringIO()
        album_df.to_csv(album_buffer,index=False)
        album_content =album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_key ="transformed-data/artist-data/"+str(datetime.now()) +".csv"

        artist_buffer =StringIO()
        artists_df.to_csv(artist_buffer,index=False)
        artist_content =artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    s3_resource =boto3.resource("s3")
    for key in spotify_keys: 
        copy_source={
            'Bucket':Bucket,
            'Key':key
        }
        s3_resource.meta.client.copy(copy_source,Bucket, 'raw-data/processed/' +key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
        
