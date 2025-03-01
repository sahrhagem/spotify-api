from glob import glob
import json
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import pandas as pd
import os
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()


class EnvNotSet(Exception):
    """
    Exception that spotify ENVs not set
    """

#Loading Spotify Credentials

# Spotify API credentials
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
if SPOTIPY_CLIENT_ID is None:
    raise EnvNotSet("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
if SPOTIPY_CLIENT_SECRET is None:
    raise EnvNotSet("SPOTIPY_CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")
if SPOTIPY_CLIENT_SECRET is None:
    raise EnvNotSet("SPOTIPY_REDIRECT_URI")
TELEGRAM_REST_ENDPOINT = os.getenv("TELEGRAM_REST_ENDPOINT")




# Init output directory
stream_dir = "./streams"
if not os.path.exists(stream_dir):
    os.makedirs(stream_dir)

export_dir = "./export"
if not os.path.exists(export_dir):
    os.makedirs(export_dir)


scope = 'user-read-recently-played'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=SPOTIPY_CLIENT_ID,
                                                client_secret=SPOTIPY_CLIENT_SECRET,
                                                redirect_uri=SPOTIPY_REDIRECT_URI,
                                                scope=scope))



def get_recently_played():
    """Fetch recently played tracks with timestamps."""
    results = sp.current_user_recently_played(limit=50)  # Get up to 50 recently played tracks
    tracks = results['items']

    print("Dump Files")
    for item in tracks:
        played_at = item['played_at']
        json_id = played_at.replace(':','_')
        json_id = json_id.replace('.','_')
        with open(f"{stream_dir}/{json_id}.json", 'w') as f:
            json.dump(item, f)


    jsonlst = []
    for fname in glob(f"{stream_dir}/*.json"):
        print(fname)
        with open(fname, 'r') as f:
            jsonlst.append(json.load(f))
    tracks = jsonlst
    #print("TrackS")
    #print(tracks)

    print("Recently played tracks:")
    song_names = []
    artists = []
    timestamps = []
    dates = []
    albums = []
    for item in tracks:
        print(item["track"])

        track = item['track']
        played_at = item['played_at']
        print(played_at)
        timestamps.append(played_at)
        song_names.append(track['name'])
        albums.append(item['track']['album']['name'])
        artists.append(', '.join(artist['name'] for artist in track['artists']))
        #dates.append(datetime.strptime(played_at, '%Y-%m-%dT%H:%M:%S.%fZ').date())  # Extract date        
        dates.append(datetime.strptime(played_at[:19], '%Y-%m-%dT%H:%M:%S').date())  # Extract date        
        #print(f"{track['name']} by {', '.join(artist['name'] for artist in track['artists'])} at {played_at}")

        json_id = played_at.replace(':','_')
        json_id = json_id.replace('.','_')
        with open(f"{stream_dir}/{json_id}.json", 'w') as f:
            json.dump(item, f)

    tracks = pd.DataFrame({"name" : song_names, "album": albums,"artist" : artists, "played_at" : timestamps, "date": dates})
    return(tracks)


tracks = get_recently_played()    
print(tracks)


# Ensure the CSV file has a header
CSV_FILE = f"{export_dir}/spotify_recently_played.csv"
# Ensure the CSV file has columns if it doesn't exist
if not os.path.exists(CSV_FILE):
    pd.DataFrame(columns=["name", "album","artist", "played_at", "date"]).to_csv(CSV_FILE, index=False, sep=";")


def update_csv_with_new_entries():
    """Fetch recently played tracks and add new entries to the CSV file."""
    # Load existing data from the CSV
    existing_df = pd.read_csv(CSV_FILE,sep=";")
    
    # Fetch new data
    new_data_df = get_recently_played()
    
    # Identify new entries by comparing the "Played At" column
    new_entries_df = new_data_df[~new_data_df["played_at"].isin(existing_df["played_at"])]

    n = len(new_entries_df)

    if not new_entries_df.empty:
        # Append new entries to the existing CSV
        new_entries_df.to_csv(CSV_FILE, mode='a', header=False, index=False, sep=";")
        print(f"Added {n} new entries to the CSV.")
    else:
        print("No new entries to add.")

    try: 
        url = f"{TELEGRAM_REST_ENDPOINT}/log"
        myobj = {'message': f"Spotify: {n} new entries"}

        x = requests.post(url, json = myobj)

        print(x.text)    
    except Exception as e:
        print(e)
update_csv_with_new_entries()


# Step 1: Read the data from the CSV file
df = pd.read_csv(CSV_FILE,sep=";")

# Step 2: Group by "Track Name" and "Date", then count occurrences
aggregated_df = (
    df.groupby(["name", "album","artist","date"])
    .size()  # Count occurrences
    .reset_index(name="count")  # Reset index and name the count column
)

# Step 3: Display the aggregated DataFrame
print(aggregated_df)



def to_smw_subobjects(df):
    """Convert a DataFrame to Semantic MediaWiki subobject syntax."""
    smw_output = ""
    for _, row in df.iterrows():
        name = row["name"]
        album = row["album"]
        artist = row["artist"]
        date = row["date"]
        play_count = row["count"]

        # Build the subobject syntax
        subobject = f"""
{{{{#subobject:
 | log=Spotify
 | date={date}
 | song={name}
 | album={album}
 | artist={artist}
 | count={play_count}
}}}}"""
        smw_output += subobject
    return smw_output

# Generate the SMW subobject text
smw_result = to_smw_subobjects(aggregated_df)

# Print the result
#print(smw_result)

# Optional: Save to a text file
with open(f"{export_dir}/smw_subobjects.txt", "w",encoding="utf-8") as f:
    f.write(smw_result)
