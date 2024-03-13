import psycopg2
import pymongo
import pandas as pd
import streamlit as st

from googleapiclient.discovery import build

api_service_name = "youtube"
api_version = "v3"
api_key= "AIzaSyCmwmMfjxTK1bsi6UZdLM-fvWr6uFDI-_s"

youtube=build(api_service_name, api_version, developerKey=api_key)

#Function to get channel details
def get_channel_info(channel_id):
 request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
 channel_data1 = request.execute()
 for i in channel_data1['items']:
  channel_information= {
    'Channel_Id': channel_data1['items'][0]["id"],
    'Channel_Name': channel_data1['items'][0]['snippet']['title'],
    'Subscription_Count': channel_data1['items'][0]['statistics']['subscriberCount'],
    'Channel_Views': channel_data1['items'][0]['statistics']['viewCount'],
    'Total_Videos': channel_data1['items'][0]['statistics']['videoCount'],
    'Channel_Description': channel_data1['items'][0]['snippet']['description'],
    'Playlist_Id':channel_data1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
  return channel_information
 


#Function to get video ids from the channels
def get_videos_ids(channel_id):
  video_ids = []
  response = youtube.channels().list(id=channel_id,
                                   part='contentDetails').execute()
  Next_page_token=None
  
  while True:
    Playlist_Id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    request=youtube.playlistItems().list(
        part='snippet',
        playlistId=Playlist_Id,
        maxResults=50,pageToken=Next_page_token).execute()
    
    for i in range (len(request['items'])):
      video_ids.append(request['items'][i]['snippet']['resourceId']['videoId'])
      Next_page_token=request.get('nextPageToken')
    if Next_page_token is None:
      break

  return video_ids
 

#function to get video details using video ids
def get_video_info(video_id_details):
  Video_data=[]
  for video_id in video_id_details:
    request = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=video_id
    )
    response = request.execute()
    for item in response['items']:
      video_information= {'Channel_Name':item['snippet']['channelTitle'],
                         'Channel_Id':item['snippet']['channelId'],
                         'Video_Id':item['id'],
                         'Title':item['snippet']['title'],
                         'Tags':item['snippet'].get('tags'),
                         'PublishedAt':item['snippet']['publishedAt'],
                         'Video_Description':item['snippet']['description'],
                         'View_Count':item['statistics'].get('viewCount'),
                         'Like_Count':item['statistics'].get('likeCount'),
                         'Favorite_Count':item['statistics'].get('favoriteCount'),
                         'Comment_Count':item['statistics'].get('commentCount'),
                         'Thumbnail':item['snippet']['thumbnails']['default']['url'],
                         'Duration':item['contentDetails']['duration'],
                         'Definition':item['contentDetails']['definition'],
                         'Caption_Status':item['contentDetails']['caption']
                         }
      Video_data.append(video_information)
  return Video_data
 


#function to comment details from video details
def get_comment_info(video_id_details):
  Comment_data=[]
  try:
    for video_id in video_id_details:
      request=youtube.commentThreads().list(
      part='snippet',
      videoId=video_id,
      maxResults=50
  )
      response = request.execute()
      for item in response['items']:
        comment_information={'Comment_Id':item['snippet']['topLevelComment']['id'],
                        'Video_Id':item['snippet']['topLevelComment']['snippet']['videoId'],
                        'Comment_Text':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Comment_Author':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_Date':item['snippet']['topLevelComment']['snippet']['publishedAt']}

        Comment_data.append(comment_information)
  except:
    pass

  return Comment_data


#function to create playlist details from channel ids
def get_playlist_info(channel_id):
  All_data=[]
  Next_page_token=None
  request=youtube.playlists().list(
      part='snippet, contentDetails',
      channelId=channel_id,
      maxResults=50,pageToken=Next_page_token
  )
  response=request.execute()
  for item in response['items']:
    playlist_information={'Playlist_Id':item['id'],
                          'Title':item['snippet']['title'],
                          'Channel_Id':item['snippet']['channelId'],
                          'Channel_Name':item['snippet']['channelTitle'],
                          'PublishedAt':item['snippet']['publishedAt'],
                          'Video_Count':item['contentDetails']['itemCount']}

    All_data.append(playlist_information)
    Next_page_token=response.get('nextPageToken')
    if Next_page_token is None:
      break
  return All_data



#MongoDB server connection
client=pymongo.MongoClient("mongodb+srv://thigilpaandi:whitedevil@prasanthhari.gfvr3gi.mongodb.net/?retryWrites=true&w=majority&appName=prasanthhari")
db=client["Youtube_project_data"]


#Calling all the objects in a function to get youtube channel details and store in MongoDB
def channel_details(channel_id):
  ch_details=get_channel_info(channel_id)
  pl_details=get_playlist_info(channel_id)
  vd_ids=get_videos_ids(channel_id)
  vd_details=get_video_info(vd_ids)
  com_details=get_comment_info(vd_ids)

  coll1=db["yt_channel_data"]
  coll1.insert_one({"yt_channel_information":ch_details,"yt_playlist_information":pl_details,"yt_video_information":vd_details,"yt_comment_information":com_details})

  return "Channel data stored in MongoDB successfully"


#FUNCTION TO CREATE CHANNEL TABLE IN SQL
def channels_table(channel_names):
    mydb = psycopg2.connect(host='localhost', user='postgres', password='whitedevil', database='youtubedata', port=5432)
    access = mydb.cursor()

    access.execute("""CREATE TABLE IF NOT EXISTS channels(
        Channel_Id varchar(70) PRIMARY KEY,
        Channel_Name varchar(100),
        Subscription_Count bigint,
        Channel_Views bigint,
        Total_Videos int,
        Channel_Description varchar(1000),
        Playlist_Id varchar(70)
    )""")

    mydb.commit()

    One_channel_list = []
    db = client["Youtube_project_data"]
    coll1 = db["yt_channel_data"]

    for yt_channel_data in coll1.find({"yt_channel_information.Channel_Name": channel_names}, {"_id": 0}):
        One_channel_list.append(yt_channel_data["yt_channel_information"])

    df_One_channel_list = pd.DataFrame(One_channel_list)

    for index, row in df_One_channel_list.iterrows():
        ch_insert_data = """
            INSERT INTO channels(Channel_Id, Channel_Name, Subscription_Count, Channel_Views, Total_Videos, Channel_Description, Playlist_Id)
            VALUES(%s, %s, %s, %s, %s, %s, %s)
            """

        value_list = (row['Channel_Id'], row['Channel_Name'], row['Subscription_Count'], row['Channel_Views'], row['Total_Videos'], row['Channel_Description'], row['Playlist_Id'])
        
        try:
            access.execute(ch_insert_data, value_list)

            mydb.commit()
        except:
           infos="This channel already exists"
           return infos


#FUNCTION TO CREATE PLAYLIST TABLE IN SQL
def Playlists_table(channel_names):
    mydb=psycopg2.connect(host='localhost',user='postgres',password='whitedevil',database='youtubedata',port=5432)
    access=mydb.cursor()

    access.execute("""CREATE TABLE IF NOT EXISTS playlists(
    Playlist_Id varchar(70) primary key,
    Title varchar(200),
    Channel_Id varchar(70),
    Channel_Name varchar(70),
    PublishedAt timestamp,
    Video_Count int
    )""")
    mydb.commit()


    One_playlist_list = []
    db=client["Youtube_project_data"]
    coll1=db["yt_channel_data"]

    for yt_channel_data in coll1.find({"yt_channel_information.Channel_Name": channel_names}, {"_id": 0}):
        One_playlist_list.append(yt_channel_data["yt_playlist_information"])

    df_One_playlist_list=pd.DataFrame(One_playlist_list[0])

    
    for index,row in df_One_playlist_list.iterrows():
        pl_insert_data="""
            insert into playlists(Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
            values(%s, %s, %s, %s, %s, %s)
            """
        
        value_list=(row['Playlist_Id'], row['Title'], row['Channel_Id'], row['Channel_Name'], row['PublishedAt'], row['Video_Count'])
        


        access.execute(pl_insert_data, value_list)

        mydb.commit()


#FUNCTION TO CREATE VIDEO TABLE IN SQL
def videos_table(channel_names):
    mydb=psycopg2.connect(host='localhost',user='postgres',password='whitedevil',database='youtubedata',port=5432)
    access=mydb.cursor()

    access.execute("""create table if not exists videos(Channel_Name varchar(100),
    Channel_Id varchar(70),
    Video_Id varchar(40) primary key,
    Title varchar(200),
    Tags text,
    PublishedAt timestamp,
    Video_Description text,
    View_Count bigint,
    Like_Count bigint,
    Favorite_Count int,
    Comment_Count int,
    Thumbnail varchar(200),
    Duration interval,
    Definition varchar(20),
    Caption_Status varchar(50)
    )""")
    mydb.commit()


    One_video_list = []
    db=client["Youtube_project_data"]
    coll1=db["yt_channel_data"]

    for yt_channel_data in coll1.find({"yt_channel_information.Channel_Name": channel_names}, {"_id": 0}):
        One_video_list.append(yt_channel_data["yt_video_information"])

    df_One_video_list=pd.DataFrame(One_video_list[0])

    for index,row in df_One_video_list.iterrows():
        vid_insert_data="""
                insert into videos(Channel_Name, Channel_Id, Video_Id, Title, Tags, PublishedAt, Video_Description, View_Count, Like_Count, Favorite_Count, Comment_Count, Thumbnail, Duration, Definition, Caption_Status)
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            
        value_list=(row['Channel_Name'], row['Channel_Id'], row['Video_Id'], row['Title'], row['Tags'], row['PublishedAt'], row['Video_Description'], row['View_Count'], row['Like_Count'], row['Favorite_Count'], row['Comment_Count'], row['Thumbnail'], row['Duration'], row['Definition'], row['Caption_Status'])
            


        access.execute(vid_insert_data, value_list)

        mydb.commit()



#FUNCTION TO CREATE COMMENTS TABLE IN SQL
def comments_table(channel_names):
    mydb=psycopg2.connect(host='localhost',user='postgres',password='whitedevil',database='youtubedata',port=5432)
    access=mydb.cursor()

    access.execute("""create table if not exists comments(Comment_Id varchar(70) primary key,
    Video_Id varchar(40),
    Comment_Text text,            
    Comment_Author varchar(100),
    Comment_Date timestamp
                )""")
    mydb.commit()

    One_comment_list = []
    db=client["Youtube_project_data"]
    coll1=db["yt_channel_data"]

    for yt_channel_data in coll1.find({"yt_channel_information.Channel_Name": channel_names}, {"_id": 0}):
        One_comment_list.append(yt_channel_data["yt_comment_information"])

    df_One_comment_list=pd.DataFrame(One_comment_list[0])


    for index,row in df_One_comment_list.iterrows():
        com_insert_data="""
                    insert into comments(Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Date)
                    values(%s, %s, %s, %s, %s)"""
                
        value_list=(row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'], row['Comment_Date'])
                

        access.execute(com_insert_data, value_list)

        mydb.commit()

def tables(one_channel):
    infos=channels_table(one_channel)
    if infos:
       return infos
    
    else:
        Playlists_table(one_channel)
        videos_table(one_channel)
        comments_table(one_channel)

        return "Table created successfully"

def view_channel_table():
    channels_list = []
    db=client["Youtube_project_data"]
    coll1=db["yt_channel_data"]

    for yt_channel_data in coll1.find({}, {"_id": 0, "yt_channel_information": 1}):
        channels_list.append(yt_channel_data['yt_channel_information'])

    # creating a dataframe to add details to streamlit for channels
    df4=st.dataframe(channels_list)
    
    return df4

def view_playlist_table():
    playlists_list = []
    db=client["Youtube_project_data"]
    coll1=db["yt_channel_data"]

    for yt_playlist_data in coll1.find({}, {"_id": 0, "yt_playlist_information": 1}):
        for i in range(len(yt_playlist_data["yt_playlist_information"])):
            playlists_list.append(yt_playlist_data['yt_playlist_information'][i])

    # Now playlists_list contains all the 'yt_playlist_information' for the channels
    df5=st.dataframe(playlists_list)

    return df5


def view_videos_table():
    videos_list=[]
    db=client['Youtube_project_data']
    coll1=db['yt_channel_data']

    for yt_videos_data in coll1.find({},{"_id":0 , "yt_video_information":1}):
        for i in range (len(yt_videos_data["yt_video_information"])):
            videos_list.append(yt_videos_data["yt_video_information"][i])

    df6=st.dataframe(videos_list)

    return df6



def view_comments_table():
    comments_list=[]
    db=client['Youtube_project_data']
    coll1=db['yt_channel_data']

    for yt_comments_data in coll1.find({},{"_id":0, "yt_comment_information":1}):
        for i in range(len(yt_comments_data["yt_comment_information"])):
            comments_list.append(yt_comments_data["yt_comment_information"][i])


    df7=st.dataframe(comments_list)

    return df7

with st.sidebar:
    st.title(":violet[Youtube data harvesting using API]")
    st.header(":green[Softwares used]")
    st.caption("**:black[Youtube]**")
    st.caption("**:brown[Visual studio code]**")
    st.caption("**:brown[MongoDB]**")
    st.caption("**:brown[PostgreSQL]**")
    st.caption("**:brown[Streamlit]**")


channel_id=st.text_input("**Enter the youtube channel id**")


if st.button("**Store data in mongoDB**"):
    ch_ids=[]
    db=client["Youtube_project_data"]
    coll1=db["yt_channel_data"]

    for yt_channel_data in coll1.find({}, {"_id": 0, "yt_channel_information": 1}):
        ch_ids.append(yt_channel_data['yt_channel_information']['Channel_Id'])
    
    if channel_id in ch_ids:
        st.success("Channel information already exists")
    
    else:
        insert_ch_id=channel_details(channel_id)
        st.success(insert_ch_id)


channels_name = []
db=client["Youtube_project_data"]
coll1=db["yt_channel_data"]
for yt_channel_data in coll1.find({}, {"_id": 0, "yt_channel_information": 1}):
    channels_name.append(yt_channel_data['yt_channel_information']["Channel_Name"])

with st.sidebar:
    single_channel = st.selectbox("**Select the channel**", channels_name)


with st.sidebar:
    if st.button("**Transfer data from mongoDB to SQL**"):
        All_tables = tables(single_channel)
        st.success(All_tables)
   

view_table=st.radio("**Select tables to view**", ("Channels", "Playlists", "Videos", "Comments"))

if view_table=="Channels":
    view_channel_table()

if view_table=="Playlists":
    view_playlist_table()

if view_table=="Videos":
    view_videos_table()

if view_table=="Comments":
    view_comments_table()

mydb=psycopg2.connect(host='localhost',user='postgres',password='whitedevil',database='youtubedata',port=5432)
access=mydb.cursor()
question=st.selectbox("**Select your question**",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
))


if question=="1. What are the names of all the videos and their corresponding channels?":
    answer1="""select Title as videoname, Channel_Name as channelname from videos"""
    access.execute(answer1)
    mydb.commit()
    q1=access.fetchall()
    df1=pd.DataFrame(q1,columns=["Video_name", "Channel_name"])
    st.write(df1)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    answer2="""select Channel_Name as channelname, Total_videos as no_of_videos from channels order by Total_videos desc"""
    access.execute(answer2)
    mydb.commit()
    q2=access.fetchall()
    df2=pd.DataFrame(q2,columns=["Channel_name", "No of videos"])
    st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    answer3="""select view_count as viewcount, channel_name as channelname from videos where view_count is not null order by viewcount desc limit 10"""
    access.execute(answer3)
    mydb.commit()
    q3=access.fetchall()
    df3=pd.DataFrame(q3,columns=["View count", "Channel name"])
    st.write(df3)

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    answer4="""select comment_count as no_of_comments, title as video_name from videos where comment_count is not null"""
    access.execute(answer4)
    mydb.commit()
    q4=access.fetchall()
    df4=pd.DataFrame(q4,columns=["No of comments", "Video title"])
    st.write(df4)

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    answer5="""select title as video_title, channel_name as channel_name, like_count as Total_likes from videos where like_count is not null order by like_count desc"""
    access.execute(answer5)
    mydb.commit()
    q5=access.fetchall()
    df5=pd.DataFrame(q5,columns=["video title", "Channel name", "Total likes"])
    st.write(df5)

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    answer6="""select like_count as Total_likes, title as video_title from videos"""
    access.execute(answer6)
    mydb.commit()
    q6=access.fetchall()
    df6=pd.DataFrame(q6,columns=["Total likes", "Video title"])
    st.write(df6)

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    answer7="""select channel_views as Channel_views, channel_name as channel_name from channels"""
    access.execute(answer7)
    mydb.commit()
    q7=access.fetchall()
    df7=pd.DataFrame(q7,columns=["Channel views", "Channel name"])
    st.write(df7)

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    answer8="""select channel_name as channel_name, title as video_name, publishedat as published_date from videos where extract(year from publishedat)=2022"""
    access.execute(answer8)
    mydb.commit()
    q8=access.fetchall()
    df8=pd.DataFrame(q8,columns=["Channel name", "Video name", "Published date"])
    st.write(df8)

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    answer9="""select channel_name as channel_name, AVG(duration) as averageduration from videos group by channel_name"""
    access.execute(answer9)
    mydb.commit()
    q9=access.fetchall()
    df9=pd.DataFrame(q9,columns=["Channel_name", "Avg_duration"])

    Q9=[]
    for index,row in df9.iterrows():
        channel_title=row['Channel_name']
        average_duration=row['Avg_duration']
        str_average_duration=str(average_duration)
        Q9.append(dict(channeltitle=channel_title, average_duration=str_average_duration))
    df=pd.DataFrame(Q9)
    st.write(df)

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    answer10="""select title as video_name, comment_count as no_of_comments, channel_name as channel_name from videos where comment_count is not null order by comment_count desc"""
    access.execute(answer10)
    mydb.commit()
    q10=access.fetchall()
    df10=pd.DataFrame(q10,columns=["video name", "No of comments", "Channel name"])
    st.write(df10)

