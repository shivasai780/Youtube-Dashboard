import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
import pandas as pd
from googleapiclient.discovery import build
import mysql.connector
from bson import ObjectId
import re
import plotly.express as px
from PIL import Image
import requests
from io import BytesIO
from datetime import datetime
import isodate
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

#set the configuration of the Page
st.set_page_config(
    page_title="Youtube Data Analysis by Shivasai",
    page_icon="✨",
    menu_items={
        'About': "# This is Youtube Analytics app Which was developed by Shiva sai!"
    }
)

# Load YouTube icon
icon_url = "https://upload.wikimedia.org/wikipedia/commons/4/42/YouTube_icon_%282013-2017%29.png"
response = requests.get(icon_url)
youtube_icon = Image.open(BytesIO(response.content))

# Set the layout with YouTube icon and title
st.markdown(
    """
    <div style="display: flex; align-items: center;">
        <img src="https://upload.wikimedia.org/wikipedia/commons/4/42/YouTube_icon_%282013-2017%29.png" width="50" style="margin-right: 10px;">
        <h1 style="color: white; margin: 0;">YouTube Analytics</h1>
    </div>
    """,
    unsafe_allow_html=True
)

#Youtube Api
api_key="AIzaSyAIb5-FNLwMpK1T7JBuVLSzsZmKazo4SdY"
youtube = build('youtube', 'v3', developerKey=api_key)

#connect to mongodb
con = pymongo.MongoClient("mongodb://localhost:27017/")
db=con['Project1_Youtube_Final']
col=db['youtube_data_collection']



#Conecting to MySQL
mydb = mysql.connector.connect(host='localhost',
                               user='root',
                               password='Shiva@607'
                              )
#Creating Cursor 
mycursor = mydb.cursor()
#Switching to the DB Created
sql="USE Project1_Youtube"
mycursor.execute(sql)


if "channel_id" not in st.session_state:
    st.session_state["channel_id"]=""


# Channel Basic Details
def channel_details(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id
    )
    response = request.execute()
    channel_data = dict(channel_name = response['items'][0]['snippet']['title'],
    channelImage_url = response['items'][0]['snippet']['thumbnails']['high']['url'],
    channel_id = response['items'][0]['id'],                    
    channel_des = response['items'][0]['snippet']['description'],
    channel_pat = response['items'][0]['snippet']['publishedAt'],
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
    channel_views = response['items'][0]['statistics']['viewCount'],
    subscription_count = response['items'][0]['statistics']['subscriberCount'],
    video_count = response['items'][0]['statistics']['videoCount'])    
    return channel_data

#get the videos Ids
def get_videoIds(Playlist_id):
    video_ids=[]
    request=youtube.playlistItems().list(
        part="contentDetails",
        playlistId=Playlist_id,
        maxResults=50)
    responce=request.execute()
    for i in range(len(responce["items"])):
        video_ids.append(responce["items"][i]["contentDetails"]["videoId"])#get the video details for the first 50 
        
    next_page_token=responce.get("nextPageToken")#if the value is present then it will return the value otherwise it will return none
    more_pages=True#change it ti true if you want toatal 
   
    while more_pages:
        
        if(next_page_token)is None:
            more_pages=False
        else:
            
            request=youtube.playlistItems().list(
                                    part="contentDetails",
                                    playlistId=Playlist_id,
                                    maxResults=50,
                                    pageToken=next_page_token#for every response of the 50 videos you will get the next page token
    
                                )
            responce=request.execute()#again get the next 50 
            for i in range(len(responce["items"])):
                video_ids.append(responce["items"][i]["contentDetails"]["videoId"])#store that in to the video ids
        
            next_page_token=responce.get("nextPageToken")#if the value is present then it will return the value otherwise it
            
        
    return video_ids 

#get the video_details
def get_video_details(video_ids):
    videos_data={}
    
    for i in range(0,len(video_ids),50):#it will produce [0,50,100,150,..until last one]
        request=youtube.videos().list(part='snippet,contentDetails,statistics',
                                id=','.join(video_ids[i:i+50]))
        response=request.execute()
        
        for video in response["items"]:
            video_id=video["id"]
            video_stats=dict(
                video_id = video['id'],
                Title=video["snippet"]["title"],
                Published_date=video["snippet"]["publishedAt"],
                Views=video["statistics"].get("viewCount"),
                commentCounts=video["statistics"].get("commentCount"),
                Likes=video["statistics"].get('likeCount'),
                favorates=video["statistics"].get("favoriteCount"),
                comments=video["statistics"].get("commentCount"),
                tags=video["snippet"].get('tags'),
                duration = video['contentDetails']['duration'],
                video_channel = video['snippet']['channelTitle'],
                video_channel_id = video['snippet']['channelId'],
                video_description = video['snippet'].get('description'),
                video_thumbnail = video['snippet']['thumbnails']['default']['url'],
                video_captsts = video['contentDetails']['caption']
                )
            
            videos_data[video_id]=video_stats#we will get the details of all that 50 video details
    
    return videos_data

#Comments details
def comment_details(v_id):
    comments={}
    for i in v_id:
        c_request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=i,
        maxResults=1
        )
        try:
            c_response = c_request.execute()
        except Exception as e:
            print(e)
            continue
        
        for i in c_response["items"]:
            c_id=i['id']
            c_txt=i['snippet']['topLevelComment']['snippet']['textDisplay']
            c_aut=i['snippet']['topLevelComment']['snippet']['authorDisplayName']
            c_publ=i['snippet']['topLevelComment']['snippet']['publishedAt']
            c_vid=i['snippet']['videoId']
            comment_data = {
                'Comment_Id':c_id,
                'Comment_Text':c_txt,
                'Comment_Author':c_aut,
                'Comment_PublishedAt':c_publ,
                'Comment_Vd_ID':c_vid
            }
            comments[c_id]=comment_data
    return comments

#Extract data From Youtube and store in the MongoDB
def main(channel_id):
    channel_data=channel_details(channel_id)#His viewcount,Followercount,Playlist ID
    print("Came Until 1")

    playlist_id = channel_data['playlist_id']#get the playlistId
    print("Came Until 2")

    video_ids=get_videoIds(playlist_id)#
    print("Came Until 2")
  
    video_data = get_video_details(video_ids)
    print("Came Until 2")
     
    final_data={
        'Channel Details':channel_data,
        'Video Details':video_data,
        # 'Comment Details':comment_data
    }
    return final_data


# Data Warehousing
# To MongoDB
# Function to add data to MongoDB
def data_to_mongodb(data):
    try:
        con = pymongo.MongoClient("mongodb://localhost:27017/")
        #Database for storing the data
        db=con['Project1_Youtube_Final']
        #Single Collection for storing the data
        col=db['youtube_data_collection']
        col.insert_one(data)
        return "Data is Successfully Uploaded in MongoDB"
    except Exception as e:
        print(f"Error is : {e}")
    finally:    
        con.close()


# Function to extract data and display channel details
def extract_and_display(channel_id):
    channel_data = main(channel_id)
    details=channel_data["Channel Details"]   
    st.image(details['channelImage_url'], width=150)  
    st.markdown(f"## **{details['channel_name']}**")
    st.write(f"**Channel Name:** {details['channel_name']}")
    st.write(f"**Channel Description:** {details['channel_des']}")
    st.write(f"**Published At:** {details['channel_pat']}")
    st.write(f"**Subscribers:**: {details['subscription_count']}")
    st.write(f"**Total Views:**: {details['channel_views']}")
    st.write(f"**Total Videos:** {details['video_count']}")
    return channel_data


def extract_data_section():
    channel_id = st.text_input("Enter YouTube Channel ID", value="")
    st.session_state["channel_id"]=channel_id
    print(f"session state---->{st.session_state["channel_id"]}")
    
    if st.button("Extract Data"):
        if channel_id:
            channel_data = extract_and_display(channel_id)
            st.session_state['channel_data'] = channel_data
        else:
            st.error("Please enter a valid YouTube Channel ID.")

    if st.button("Move to MongoDB"):
        if 'channel_data' in st.session_state:
            query={"Channel Details.channel_id": channel_id}
            if col.find_one(query):
                st.warning("The Channel Details are present in the MongoDB")
            else:
                message = data_to_mongodb(st.session_state['channel_data'])
                st.success(message,icon="✅")
        else:
            st.error("No data to move. Please extract data first.")

def get_cNames():
    channelnames=[]
    records=col.find()

    for document in records:
        channelnames.append(document['Channel Details']['channel_name'])
    
    return(channelnames)

def convert_to_dataframes(name):
   print("came here")
   for document in col.find({'Channel Details.channel_name':name}):
       document_id=document["_id"]

   for document in col.find({'_id':document_id},{'Channel Details':1,'_id':0}):#find the if with i and include the Channel Details and exclude the _id
        channel_details_list = [document['Channel Details']]
   channel_df=pd.DataFrame(channel_details_list)


   for document in col.find({'_id':document_id},{'Video Details':1,'_id':0}):
       video_details_list=document["Video Details"]
   video_df = pd.DataFrame.from_dict(video_details_list,orient='index')
#    st.dataframe(video_df)
    
   
   return channel_df,video_df
       
def insert_channel_SQL(df_ch):
    df_ch["channel_views"]= pd.to_numeric(df_ch["channel_views"],errors="coerce")
    channel_data = df_ch[['channel_id','channel_name','channel_views','channel_des']]
    channel_data_to_insert = [tuple(row) for row in channel_data.values]
    print(f'channel dataframes----->',channel_data_to_insert)
      # Inserting Channel Data to MySQL
    try:
        for row in channel_data_to_insert:
            channel_id = row[0]
            # Check if the channel_id already exists
            check_sql = "SELECT COUNT(*) FROM Channel WHERE channel_id = %s"
            mycursor.execute(check_sql, (channel_id,))
            count = mycursor.fetchone()[0]

            if count > 0:
                print(f"Channel details for ID {channel_id} already exist.")
                # Optionally, display a message in Streamlit
                st.warning(f"Channel details for ID {channel_id} already exist.")
            else:
                # If not exist, insert the new record
                insert_sql = "INSERT INTO Channel(channel_id, channel_name, channel_views, channel_des) VALUES(%s,%s,%s,%s)"
                mycursor.execute(insert_sql, row)
        
        mydb.commit()

    except Exception as e:
        print("Error inserting data: ", e)
        # Optionally, display the error in Streamlit
        st.error(f"Error inserting data: {e}")


def insert_video_SQL(df_video):
    #Fill the null value
    try:
        df_video['Views'] = pd.to_numeric(df_video['Views'],errors='coerce').fillna(0).astype(int)
        df_video['Likes'] = pd.to_numeric(df_video['Likes'],errors='coerce').fillna(0).astype(int)
        df_video['favorates'] = pd.to_numeric(df_video['favorates'],errors='coerce').fillna(0).astype(int)
        df_video['commentCounts'] = pd.to_numeric(df_video['commentCounts'],errors='coerce').fillna(0).astype(int)
    except Exception as e:
        print(e)

    #convert to the correct data types
    #Format the Date
    df_video['Published_date'] = pd.to_datetime(df_video['Published_date'], format='%Y-%m-%dT%H:%M:%SZ')
    df_video['duration'] = df_video['duration'].apply(lambda x: isodate.parse_duration(x).total_seconds())
    video_data = df_video[['video_id', 'Title', 'video_description', 'Published_date', 'Views', 'Likes', 'favorates', 'commentCounts', 'duration', 'video_thumbnail', 'video_captsts','video_channel']]
    vdata_to_insert = [tuple(row) for row in video_data.values]
    #Inserting Video Data to MySQL
    try:
        sql="INSERT INTO Video(video_id, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration, thumbnail, caption_status, video_channel) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        mycursor.executemany(sql,vdata_to_insert)
        mydb.commit()
    except Exception as e:
        print("Data already Present : ",e)
    



def PresprocessData():
    st.header("Preprocess And Migrate The Data To SQL")
    channel_name=get_cNames()
    print(f"channel_names----->{channel_name}")
    # name_tab = st.selectbox("Choose the Name", [name for name in channel_name], index=0, key="name_tab")
    name_tab=st.selectbox("Choose the channel for which you intend to transfer data from MongoDB to MySQL.",(channel_name))

    df_channel,df_video =convert_to_dataframes(name_tab)
    if st.button("Migrate Data"):
        insert_channel_SQL(df_channel)
        insert_video_SQL(df_video)
        st.success("SuccessFully Migrated To SQL",icon="✅")


# Streamlit application layout
st.markdown("<br>", unsafe_allow_html=True)



# Creating tabs
tabs = st.tabs(["Extract Data", "Preprocessing", "Data Analytics"])

# Extract Data Tab
with tabs[0]:
    extract_data_section()

# Preprocessing Tab
with tabs[1]:
    
    st.write("Preprocessing")
    PresprocessData()

# Data Analytics Tab
with tabs[2]:
    st.write("Data Analytics functionality goes here.")
    st.header("Data Analysis and Insights")
    qs = ["1. What are the names of all the videos and their corresponding channels?",
                "2. Which channels have the most number of videos, and how many videos do they have?",
                "3. What are the top 10 most viewed videos and their respective channels?",
                "4. How many comments were made on each video, and what are their corresponding video names?",
                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                "6. What is the total number of likes for each video, and what are their corresponding video names?",
                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                "8. What are the names of all the channels that have published videos in the year 2022?",
                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ]
    qn = st.selectbox(
            'Select any questions from below to gain insights',
            (qs))
    if qn == qs[0]:
        query="SELECT video_name,video_channel FROM project1_youtube.video ORDER BY video_channel"
        mycursor.execute(query)
        data = mycursor.fetchall()
        columns = [col[0] for col in mycursor.description]
        # Convert the fetched data into a pandas DataFrame
        df = pd.DataFrame(data, columns=columns)
        data_df=df.set_index(pd.Index(range(1,len(data)+1)))
        # Display the DataFrame
        st.title("List Of All Videos")
        with st.container():
                st.subheader("Data Table")
                st.write(data_df)
    if qn==qs[1]:
        query="select video_channel,count(video_id) as Videocount From project1_youtube.video group by video_channel"
        mycursor.execute(query)
        data=mycursor.fetchall()
        columns = [col[0] for col in mycursor.description]
        # Convert the fetched data into a pandas DataFrame
        df = pd.DataFrame(data, columns=columns)
        # Display the DataFrame
        st.title("Count Of All Videos")
        fig = px.bar(df, 
                    x="video_channel", 
                    y="Videocount", 
                    color="Videocount",  # Color bars based on the video count
                    labels={"video_channel": "Video Channel", "Videocount": "Video Count"},
                    title="Video Count by Channel",
                    color_continuous_scale=px.colors.sequential.Darkmint_r)  # Choose a color scale

        # Update the layout for better appearance
        fig.update_layout(xaxis_tickangle=-45)  # Rotate x-axis labels for better readability

        # Display the Plotly chart in Streamlit
        st.subheader("Video Count Bar Chart")
        st.plotly_chart(fig)
    if qn==qs[2]:
        query="SELECT video_channel,video_name,view_count FROM  project1_youtube.video order by view_count desc limit 10"
        mycursor.execute(query)
        data=mycursor.fetchall()
        columns = [col[0] for col in mycursor.description]
        # Convert the fetched data into a pandas DataFrame
        df = pd.DataFrame(data, columns=columns)
        # Display the DataFrame
        st.title("Top 10 videos")
        print(f"index is----->{pd.Index(range(1,len(data)+1))}")
        data_df = pd.DataFrame(data,columns=columns).set_index(pd.Index(range(1, len(data) + 1)))

        #data_df.index = range(1, len(data_df) + 1)
        st.title("Top 10 Most Viewed Videos")
        with st.container():
            st.subheader("Channel vs Number Of Videos")
            fig = px.bar(df, 
             x='video_name', 
             y='view_count', 
             color='video_channel', 
             title='Top 10 Most Viewed Videos',
             labels={'view_count':'View Count', 'video_name':'Video Name'},
             text="view_count"
             )
            fig.update_layout(height=600,  # Increase the height (default is 450)
                  width=1000,xaxis_tickangle=90,
                  xaxis=dict(
                         tickfont=dict(size=10),  # Adjust font size
                          title_standoff=100,  # Increase the distance between labels and axis
                     )
                  )
            

            # Display the chart in Streamlit
            st.plotly_chart(fig)
    
    if qn==qs[4]:
        query="SELECT  video_channel, video_name, like_count FROM  project1_youtube.video order by  like_count desc"
        mycursor.execute(query)
        data=mycursor.fetchall()
        columns = [col[0] for col in mycursor.description]
        # Convert the fetched data into a pandas DataFrame
        df = pd.DataFrame(data, columns=columns)
        # Display the DataFrame
        print(f"index is----->{pd.Index(range(1,len(data)+1))}")
        data_df = pd.DataFrame(data,columns=columns).set_index(pd.Index(range(1, len(data) + 1)))

        
        with st.container():
            st.title("Top liked Videos")
            st.dataframe(data_df)

    if qn==qs[5]:
        query="SELECT  video_name, like_count FROM  project1_youtube.video"
        mycursor.execute(query)
        data=mycursor.fetchall()
        columns = [col[0] for col in mycursor.description]
        # Convert the fetched data into a pandas DataFrame
        df = pd.DataFrame(data, columns=["video_name","like_count"])
        # Display the DataFrame
        print(f"index is----->{pd.Index(range(1,len(data)+1))}")
        data_df = pd.DataFrame(data,columns=columns).set_index(pd.Index(range(1, len(data) + 1)))
        with st.container():
            st.title("Top liked Videos")
            st.dataframe(data_df)
    if qn==qs[6]:
        query="select channel_views,channel_name from project1_youtube.channel "
        mycursor.execute(query)
        data=mycursor.fetchall()
        columns=[col[0] for col in mycursor.description]
        df=pd.DataFrame(data,columns=columns)
        data_df = pd.DataFrame(data,columns=columns).set_index(pd.Index(range(1, len(data) + 1)))
        with st.container():
            st.title("Top liked Videos")
            st.dataframe(data_df)
    if qn==qs[7]:
        query="select distinct video_channel   from project1_youtube.video where year(published_date)=2022"
        mycursor.execute(query)
        data=mycursor.fetchall()
        columns=[col[0] for col in mycursor.description]
        df=pd.DataFrame(data,columns=columns)
        data_df = pd.DataFrame(data,columns=columns).set_index(pd.Index(range(1, len(data) + 1)))
        with st.container():
            st.title("Top liked Videos")
            st.dataframe(data_df)
    if qn==qs[8]:
        query="select video_channel,avg(duration) from project1_youtube.video group by video_channel"
        mycursor.execute(query)
        data=mycursor.fetchall()
        columns=[col[0] for col in mycursor.description]
        df=pd.DataFrame(data,columns=columns)
        data_df = pd.DataFrame(data,columns=columns).set_index(pd.Index(range(1, len(data) + 1)))
        with st.container():
            st.subheader("Average Duration of Channels")
            fig = px.bar(df, 
             x='video_channel', 
             y='avg(duration)', 
             color='video_channel', 
             title='Average Duration of Channels',
             labels={'video_channel':'Channel Name', 'avg(duration)':'Average Duration'},
             text="avg(duration)"
             )
            fig.update_layout(height=600,  # Increase the height (default is 450)
                  width=1000,xaxis_tickangle=90,
                  xaxis=dict(
                         tickfont=dict(size=10),  # Adjust font size
                          title_standoff=80,  # Increase the distance between labels and axis
                     )
                  )
            

            # Display the chart in Streamlit
            st.plotly_chart(fig)


            





    

    

