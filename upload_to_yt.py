import os
import time
import pandas as pd
import json
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors


CLIENT_SECRETS_FILE = "client_secret.json"
REDIRECT_URI = "http://localhost:8080"
SCOPES = ["https://www.googleapis.com/auth/youtube"]
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"


# Authorize the request and store authorization credentials.
def get_authenticated_service():
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, SCOPES, redirect_uri=REDIRECT_URI)
    credentials = flow.run_local_server()
    return googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, credentials=credentials)


def upload_video(youtube, video_file, title, description, category_id, keywords, privacy_status):
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": keywords,
            "categoryId": category_id
        },
        "status": {
            "privacyStatus": privacy_status
        }
    }

    # Call the API's videos.insert method to create and upload the video.
    insert_request = youtube.videos().insert(
        part=",".join(body.keys()),
        body=body,
        media_body=googleapiclient.http.MediaFileUpload(video_file, chunksize=-1, resumable=True)
    )

    response = None
    error = None
    retry = 0
    while response is None:
        try:
            print(f"Uploading file {video_file}")
            status, response = insert_request.next_chunk()
            if response is not None:
                if 'id' in response:
                    print(f"Video id '{response['id']}' was successfully uploaded.")
                    return response['id']
                else:
                    exit(f"The upload failed with an unexpected response: {response}")
        except googleapiclient.errors.HttpError as e:
            if e.resp.status in [500, 502, 503, 504]:
                error = f"A {e.resp.status} error occurred: {e}"
                retry += 1
            else:
                raise

        if error is not None:
            print(error)
            print(f"Sleeping for {2 ** retry} and then retrying...")
            time.sleep(2 ** retry)


def create_playlist(youtube, title, description, privacy_status):
    playlists_insert_response = youtube.playlists().insert(
        part="snippet,status",
        body=dict(
            snippet=dict(
                title=title,
                description=description
            ),
            status=dict(
                privacyStatus=privacy_status
            )
        )
    ).execute()
    return playlists_insert_response["id"]


def add_video_to_playlist(youtube, video_id, playlist_id):
    youtube.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id
                }
            }
        }
    ).execute()


# Function to check if the playlist exists and return its ID, or create a new one
def get_or_create_playlist(youtube, playlist_title, playlist_description, privacy_status):
    # Fetch existing playlists and see if ours exists
    response = youtube.playlists().list(mine=True, part='snippet').execute()
    for playlist in response['items']:
        if playlist['snippet']['title'] == playlist_title:
            return playlist['id']

    # No matching playlist, so create a new one
    return create_playlist(youtube, playlist_title, playlist_description, privacy_status)


def find_video_by_title(youtube, title):
    search_response = youtube.search().list(
        q=title,
        part="id,snippet",
        maxResults=1,
        type="video",
        forMine=True  # This restricts the search to the authenticated user's channel
    ).execute()

    items = search_response.get('items')
    if not items:
        return None  # No video found by that title

    # Assuming we want only the first result to match the given title exactly
    for item in items:
        if item['snippet']['title'].strip().lower() == title.strip().lower():
            return item['id']['videoId']

    return None  # No video found by that title or exact match not found


def is_video_in_playlist(youtube, video_id, playlist_id):
    # Retrieve the list of videos in the playlist
    playlist_videos_request = youtube.playlistItems().list(
        playlistId=playlist_id,
        part="snippet",
        maxResults=50
    )

    while playlist_videos_request:
        playlist_videos_response = playlist_videos_request.execute()

        for playlist_item in playlist_videos_response['items']:
            if playlist_item['snippet']['resourceId']['videoId'] == video_id:
                return True  # The video is already in the playlist

        playlist_videos_request = youtube.playlistItems().list_next(
            playlist_videos_request, playlist_videos_response)

    return False  # The video was not found in this playlist


def load_or_create_dict_from_json(file_path: str) -> dict:
    # Create file if it does not exist
    try:
        with open(file_path, 'x') as file:
            json.dump({}, file)
    except FileExistsError:
        print(f"The file '{file_path}' already exists. Loading file")

    with open(file_path, 'r') as file:
        try:
            jdict = json.load(file)
        # Catch empty files
        except json.JSONDecodeError:
            # init dict
            jdict = {}
            pass
    return jdict


def has_file(file_path: str) -> bool:
    if os.path.exists(file_path):
        return True
    else:
        print(f"The file '{file_path}' does not exist.")
        return False


if __name__ == '__main__':
    # When running locally, disable OAuthlib's HTTPs verification.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    youtube_service = get_authenticated_service()

    # Load video data
    vid_dir_path = 'vids/'
    vids = pd.read_csv('neuroai_videos.csv')

    # Set id columns to str type
    vids['video_id'] = vids['video_id'].astype(str)
    vids['playlist_id'] = vids['playlist_id'].astype(str)

    uploads = load_or_create_dict_from_json('upload_ids.json')
    upload_titles = uploads.keys()
    playlists = load_or_create_dict_from_json('playlist_ids.json')
    playlist_names = playlists.keys()
    for i, vid in vids.iterrows():
        # Check for empty entries
        if not type(vid['video_file']) is str:
            continue
        video_file = vid_dir_path + vid['video_file']
        title = vid['title']
        description = vid['description']
        # convert float from spreadsheet to int then str
        category_id = str(int(vid['category_id']))
        keywords = [kw.strip() for kw in vid['keywords'].split(',')]
        privacy_status = vid['privacy_status']
        playlist_name = vid['playlist_name']
        playlist_description = vid['playlist_description']
        playlist_privacy_status = vid['playlist_privacy_status']
        video_id = vid['video_id']
        playlist_id = vid['playlist_id']

        print(f'\nUploading video: {title}')

        # Check for video_file
        if not has_file(video_file):
            continue

        # Check upload record
        if title in upload_titles:
            video_id = uploads[title]
            print(f'Video {title} already uploaded with id: {video_id}')
        else:
            # Check if video with the same title already exists
            existing_video_id = find_video_by_title(youtube_service, title)

            if existing_video_id:
                print(f'Video already exists with ID: {existing_video_id}')
                video_id = existing_video_id
            else:
                # No video with the same title exists, proceed with upload
                video_id = upload_video(
                    youtube_service,
                    video_file,
                    title,
                    description,
                    category_id,
                    keywords,
                    privacy_status
                )
                print(f'Uploaded new video with ID: {video_id}')
            # Update record
            uploads[title] = video_id
        # update csv
        vids.loc[i, 'video_id'] = video_id

        # Check playlist record
        if playlist_name in playlist_names:
            playlist_id = playlists[playlist_name]['id']
            playlist_vid_ids = playlists[playlist_name]['vid_ids']
            print(f'Playlist {playlist_name} already exists with id {playlist_id}')
            # Check if video in playlist
            if video_id in playlist_vid_ids:
                print(f'Video {title} already in playlist {playlist_name}')
            else:
                # If the video is not in the playlist, add it
                add_video_to_playlist(youtube_service, video_id, playlist_id)
                print(f"Video {video_id} has been added to the playlist {playlist_id}.")
                # update record
                playlists[playlist_name]['vid_ids'].append(video_id)
        else:
            # After successful upload, get the playlist ID or create the playlist
            playlist_id = get_or_create_playlist(youtube_service, playlist_name, playlist_description, playlist_privacy_status)

            # Check if the video is already in the playlist
            if is_video_in_playlist(youtube_service, video_id, playlist_id):
                print(f"Video {video_id} is already in the playlist.")
            else:
                # If the video is not in the playlist, add it
                add_video_to_playlist(youtube_service, video_id, playlist_id)
                print(f"Video {video_id} has been added to the playlist {playlist_id}.")
            # Update record
            playlists[playlist_name] = {}
            playlists[playlist_name]['id'] = playlist_id
            playlists[playlist_name]['vid_ids'] = [video_id]
        # Update csv
        vids.loc[i, 'playlist_id'] = playlist_id

    # Write records to file and update csv
    vids.to_csv('neuroai_videos_with_ids.csv', index=False)
    with open('upload_ids.json', 'w') as file:
        json.dump(uploads, file)
    with open('playlist_ids.json', 'w') as file:
        json.dump(playlists, file)
