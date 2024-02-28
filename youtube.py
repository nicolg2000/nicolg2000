import googleapiclient.discovery
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime
from datetime import timedelta

api_key = "AIzaSyBYr044X0ps9F1XwuHvG8eblcaHky4ey4Q"

api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)


#to get channel details

def get_channel_details(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    
    for i in response['items']:
        data = dict(Channel_Name=i['snippet']['title'],
                        Channel_Id=i[ 'id'],
                        Subscribers=i['statistics']['subscriberCount'],
                        Views=i['statistics']['viewCount'],
                        Total_Videos=i['statistics']['videoCount'],
                        Channel_description=i['snippet']['description'],
                        Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                       )
        return data
    
    
#to get vedio ids
    
def get_video_Ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(
                part="contentDetails",
                id=channel_id).execute()
    playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken') 

        if next_page_token is None:
            break
    return video_ids      
        
    
#get vedio informations

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        
        
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                     Channel_Id=item['snippet']['channelId'],
                     video_Id=item['id'],
                     Title=item['snippet']['title'],
                     Thumbnail=item['snippet']['thumbnails']['default']['url'],
                     Description=item['snippet'].get('description'),
                     Published_Date=item['snippet']['publishedAt'],
                     Views=item['statistics'].get('viewCount'),
                     Likes=item['statistics'].get('likeCount'),
                     Comments=item['statistics'].get('commentCount'),
                     Favorite_Count=item['statistics']['favoriteCount'],
                     Definition=item['contentDetails']['definition'],
                     Caption_Status=item['contentDetails']['caption']
                     )
            video_data.append(data)
    return video_data      
         
              
        

#getting comment details

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
            
            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                         Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                         )
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data   
    

#mongo inserting


client = MongoClient("mongodb+srv://nicolgmundamattom:3v3jcgXRacP0o3Oa@cluster0.nsntngc.mongodb.net/")


db=client['Youtube_data']

#getting entire channel details

def channel_details(channel_id):
    ch_details=get_channel_details(channel_id)
    vi_ids=get_video_Ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({'channel_information':ch_details,'video_information':vi_details,
                     'comment_information':com_details})
    return 'upload successfull'
    
    
#insert to mysql

connection = mysql.connector.connect(host="localhost",user="root",password="",database="youtube_youtube")
mycursor=connection.cursor()

#getting table for channel details

def Channel_table():
    connection = mysql.connector.connect(host="localhost",user="root",password="",database="youtube_youtube")
    mycursor=connection.cursor()
    
    drop_query='drop table if exists Channels'
    mycursor.execute(drop_query)
    connection.commit()
    try:
        query='create table Channels(ID INT AUTO_INCREMENT PRIMARY KEY,Channel_Name varchar(100),Channel_Id varchar(80),Subscribers bigint,Views bigint,Total_Videos int,Channel_description text,Playlist_Id varchar(80) )'
        mycursor.execute(query)
        connection.commit()
    
    except:
        print('Channel table already created')
    
    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)
    
    for index,row in df.iterrows():
        insert_query='insert into Channels(Channel_Name,Channel_Id,Subscribers,Views,Total_Videos,Channel_description,Playlist_Id)values(%s,%s,%s,%s,%s,%s,%s)'
    
        values=(row['Channel_Name'],row['Channel_Id'],row['Subscribers'],row['Views'],row['Total_Videos'],row['Channel_description'],row['Playlist_Id'])
        try:
            mycursor.execute(insert_query,values)
            connection.commit()
    
        except:
            print('Channel details already inserted')
            
#getting table for video details

def video_table():
    connection = mysql.connector.connect(host="localhost", user="root", password="", database="youtube_youtube")
    mycursor = connection.cursor()
    
    # Drop the existing table if it exists
    drop_query = 'DROP TABLE IF EXISTS Videos'
    mycursor.execute(drop_query)
    connection.commit()
    
    # Create the new table structure with the desired changes
    query = '''
    CREATE TABLE Videos (ID INT AUTO_INCREMENT PRIMARY KEY,
        Channel_Name VARCHAR(100),
        Channel_Id VARCHAR(100),
        video_Id VARCHAR(30),
        Title VARCHAR(150),
        Thumbnail VARCHAR(200),
        Description TEXT,
        Published_Date TIMESTAMP,
        Views BIGINT,
        Likes BIGINT,
        Comments INT,
        Favorite_Count INT,
        Definition VARCHAR(10),
        Caption_Status VARCHAR(50)
    )
    '''
    mycursor.execute(query)
    connection.commit()
    
    
    vi_list=[]
    db = client['Youtube_data']
    coll1 = db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df1=pd.DataFrame(vi_list)
    
    for index, row in df1.iterrows():
        insert_query = 'INSERT INTO Videos(Channel_Name, Channel_Id, video_Id, Title, Thumbnail, Description, Published_Date, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        

        from datetime import datetime
        import pytz

        published_date_str = row['Published_Date']
        published_date_utc = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
        published_date_india = published_date_utc.astimezone(pytz.timezone('Asia/Kolkata'))
        formatted_date = published_date_india.strftime('%Y-%m-%d %H:%M:%S')


        values = (row['Channel_Name'], row['Channel_Id'], row['video_Id'], row['Title'], row['Thumbnail'], row['Description'], formatted_date, row['Views'], row['Likes'], row['Comments'], row['Favorite_Count'], row['Definition'], row['Caption_Status'])
        
        try:
            mycursor.execute(insert_query, values)
            connection.commit()
        except mysql.connector.IntegrityError as e:
            print(f"Skipping duplicate entry for video_Id: {row['video_Id']}")

# getting table for comment details

def comments_table():
    connection = mysql.connector.connect(host="localhost",user="root",password="",database="youtube_youtube")
    mycursor=connection.cursor()
    
    drop_query='drop table if exists Comments'
    mycursor.execute(drop_query)
    connection.commit()
    
    query='create table Comments(ID INT AUTO_INCREMENT PRIMARY KEY,Comment_Id varchar(100),Video_Id varchar(50),Comment_Text text,Comment_Author varchar(150),Comment_Published timestamp)'
    mycursor.execute(query)
    connection.commit()
    
    com_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df2=pd.DataFrame(com_list)
    
    
    
    connection = mysql.connector.connect(host="localhost", user="root", password="", database="youtube_youtube")
    mycursor = connection.cursor()
    
    for index, row in df2.iterrows():
        insert_query = 'INSERT INTO Comments(Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published) VALUES (%s, %s, %s, %s, %s)'
        
        # Convert the datetime string to MySQL datetime format
        comment_published_str = row['Comment_Published']
        comment_published = datetime.strptime(comment_published_str, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    
        values = (row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'], comment_published)
        
        try:
            mycursor.execute(insert_query, values)
            connection.commit()
        except mysql.connector.IntegrityError as e:
            print(f"Skipping duplicate entry for Comment_Id: {row['Comment_Id']}")


def tables():
    Channel_table()
    video_table()
    comments_table()

    return 'Table Created Successfully'

def show_channels_table():
    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)

    return df


def show_videos_table():
    vi_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df1=st.dataframe(vi_list)

    return df1



def show_comments_table():
    com_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df2=st.dataframe(com_list)

    return df2

#streamlit

st.title(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]')


with st.sidebar:
    st.title(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
    url_id=st.selectbox(':green[Get Some Channel ID]',('1.UCVcWdDdUMNs53QuUJv2SWgg',
                                              '2. UC0caLFAzXdbiR1Xc4voJr6w',
                                              '3. UCyTb2viKGIF5OnaHHpfmj9A',
                                              '4. UCwcrEPqkVFvIWJG5VJ7CpJA',
                                              '5. UCC0OcsqaRPI7_8CnlDvxupQ',
                                              '6. UCMngE7Lm5hzD3kqYpOr1P9w',
                                              '7. UC6bss7sFhuABlD5n3LJFFJQ',
                                              '8. UCP3gDBK_FiCzg9QkjB0S_Aw',
                                              '9. UCYejxXlnd0hgM2LPQun8pow',
                                              '10. UCoI7kIuTrSjwBIw1Yd3nVCQ'),
                                              index=None,
                                              placeholder='Select the Channel ID...')

channel_id=st.text_input(':green[Enter the channel ID]')


if st.button('Collect and Store Data in Mongo'):
    ch_ids=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])
    if channel_id in ch_ids:
        st.success('channel details of given id already exist')

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button('Migrate to MySQL'):
    Table=tables()
    st.success(Table)
show_table=st.radio('SELECT THE TABLE ',('CHANNELS','VIDEOS','COMMENTS'))

if show_table=='CHANNELS':
    show_channels_table()

elif show_table=='VIDEOS':
    show_videos_table()

elif show_table=='COMMENTS':
    show_comments_table()



connection = mysql.connector.connect(host="localhost",user="root",password="",database="youtube_youtube")
mycursor=connection.cursor()

question=st.selectbox(':green[Select Your Question]',(' What are the names of all the videos and their corresponding channels?',
                                              ' Which channels have the most number of videos?',
                                              ' What are the top 10 most viewed videos and their respective channels?',
                                              ' How many comments were made on each video, and what are their corresponding video names?',
                                              ' Which videos have the highest number of likes, and what are their corresponding channel names?',
                                              ' What is the total number of likes and what are their corresponding video names?',
                                              ' What is the total number of views for each channel, and what are their corresponding channel names?',
                                              ' What are the names of all the channels that have published videos in the year 2022?',
                                              ' Which videos have the highest number of comments, and what are their corresponding channel names?'))


if question==' What are the names of all the videos and their corresponding channels?':
    query1='''select title as videos,channel_name as channelname from Videos'''
    mycursor.execute(query1)
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=['video title','channel name'])
    st.write(df)


elif question==' Which channels have the most number of videos?':
    query2='''select channel_name as channelname,total_videos as no_videos from Channels 
                order by total_videos desc'''
    mycursor.execute(query2)
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=['channel name','No of videos'])
    st.write(df2) 


elif question==' What are the top 10 most viewed videos and their respective channels?':
    query3='''select views as views,channel_name as channelname,title as videotitle from Videos 
                where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=['views','channel_name','videotitle'])
    st.write(df3)  



elif question==' How many comments were made on each video, and what are their corresponding video names?':
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    mycursor.execute(query4)
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=['no of comments','videotitle'])
    st.write(df4) 

 
elif question==' Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5='''select title as videotitle,channel_name as channelname,likes as likescount from videos where likes is not null
                order by likes desc'''
    mycursor.execute(query5)
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=['videotitle','channelname','likescount'])
    st.write(df5)


elif question==' What is the total number of likes and what are their corresponding video names?':
    query6='''select likes as likescount,title as videotitle from videos'''
    mycursor.execute(query6)
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=['likescount','videotitle'])
    st.write(df6)  


elif question==' What is the total number of views for each channel, and what are their corresponding channel names?':
    query7='''select channel_name as channelname,views as totalviews from Channels'''
    mycursor.execute(query7)
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=['channelname','totalviews'])
    st.write(df7)    



elif question==' What are the names of all the channels that have published videos in the year 2022?':
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos 
                where extract(year from published_date)=2022'''
    mycursor.execute(query8)
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=['video_title','videorelease','channelname'])
    st.write(df8) 



elif question==' Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments
                is not null order by comments desc'''
    mycursor.execute(query10)
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=['videotitle','channelname','comments'])
    st.write(df10)
    



