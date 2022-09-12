from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import mysql.connector as conn
from pymongo import MongoClient
import gridfs
import requests
from pytube import YouTube
import boto3
import os




driver_path = r'C:\Users\Prashant\Desktop\python files\pycharm files\youtubescraping\chromedriver.exe'
driver = webdriver.Chrome(executable_path= driver_path)
driver.maximize_window()
keyword = keyword = ["Hitesh Choudhary","MySirG.com","Krish Naik","Telusko"]
max_link = 50


def page_scroll_down(): #to scroll the page 5 times
    element = driver.find_element('tag name', 'body')

    scroll_down = 0
    while scroll_down < 5:
        element.send_keys(Keys.PAGE_DOWN)
        time.sleep(2)
        scroll_down += 1

def  comment_scroll_down():  #scrolling through comment section
    last_count = 0

    while True:
        page_scroll_down()
        allcomments = driver.find_elements('xpath', '//*[@id="content-text"]')
        new_count = len(allcomments)
        if last_count == new_count:
            break
        last_count = new_count

#searching the keyword on youtube
def search_kw(keywords:str):
    try:
        driver.implicitly_wait(1)
        url = f"""https://www.youtube.com/results?search_query={keywords.replace(" ","+")}"""
        driver.get(url)
        yt_content = driver.page_source.encode('utf-8')
        soup = bs(yt_content,'lxml')
        link = soup.findAll('div', id = 'info-section' )
        chnnllink = link[0].a['href']
        videospage = f'https://www.youtube.com{chnnllink}/videos'
        time.sleep(2)
        return videospage
    except Exception as e:
        print("Unable to connect the YouTube channel-",e)

#fetch all the details of video.
def all_details(videospage:str, max_link:int):
    try:
        driver.get(videospage)
        time.sleep(2)
        page_scroll_down()
        page_details = driver.page_source.encode('utf-8')
        soup_page = bs(page_details,'lxml')
        alltitle = soup_page.findAll('a',id = 'video-title')
        allthumbnails = soup_page.findAll('img', class_="style-scope yt-img-shadow", width="210")
        videolink = []
        thumbnail = []
        title = []
        for i in range(max_link):
            videolink.append('https://www.youtube.com' + alltitle[i]['href'])
            thumbnail.append(allthumbnails[i]["src"])
            title.append(alltitle[i].text)
    except Exception as e:
        print("link not accessible-",e)


    likes = []
    no_of_comments = []
    commentor = []
    comment = []
    try:
        for link in videolink:
            if 'shorts' not in link:
                driver.get(link)
                time.sleep(5)
                element = driver.find_element('tag name', 'body')
                comment_scroll_down()
                element.send_keys(Keys.HOME)
                time.sleep(1)
                likes.append(driver.find_element('xpath', '//*[@id="top-level-buttons-computed"]/ytd-toggle-button-renderer[1]/a').text)
                no_of_comments.append(driver.find_element('xpath', '//*[@id="count"]/yt-formatted-string/span[1]').text)
                allcommentors = driver.find_elements('xpath','//*[@id="author-text"]')
                allheaders =  driver.find_elements('xpath', '//*[@id="author-comment-badge"]')
                allcomments = driver.find_elements('xpath','//*[@id="content-text"]')

                for txt in range(0, len(allcomments)):
                    commentor.append([link, allheaders[txt].text or allcommentors[txt].text])
                    comment.append([link, allcomments[txt].text])

            else:
                driver.get(link.replace('shorts/','watch?v='))
                time.sleep(5)
                element = driver.find_element('tag name', 'body')
                comment_scroll_down()
                element.send_keys(Keys.HOME)
                time.sleep(1)
                likes.append(driver.find_element('xpath','//*[@id="top-level-buttons-computed"]/ytd-toggle-button-renderer[1]/a').text)
                no_of_comments.append(driver.find_element('xpath', '//*[@id="count"]/yt-formatted-string/span[1]').text)
                allcommentors = driver.find_elements('xpath', '//*[@id="author-text"]')
                allheaders = driver.find_elements('xpath', '//*[@id="author-comment-badge"]')
                allcomments = driver.find_elements('xpath', '//*[@id="content-text"]')

                for txt in range(0, len(allcomments)):
                    commentor.append([link, allheaders[txt].text or allcommentors[txt].text])
                    comment.append([link, allcomments[txt].text])


        for i in range(max_link):#upload all the data to mysql, mongoDB and S3 bucket
            print(key,title[i],likes[i],no_of_comments[i],videolink[i],thumbnail[i])
            query = """insert into all_channel_details values('{}',"{}",'{}',{},'{}','{}') on duplicate key update title="{}",likes='{}',no_of_comments={},thumbnail_link='{}'""".format(key,str(title[i]),likes[i],no_of_comments[i],videolink[i],thumbnail[i],str(title[i]),likes[i],no_of_comments[i],thumbnail[i])

            execute_query(query)
            upload_thumbnails_and_comments(key,title[i],thumbnail[i],videolink[i],commentor,comment)
            upload_video_to_s3(key,videolink[i],title[i])

    except Exception as e:
        print('getting this error-', e)


def create_mysql_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection =conn.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )

    except Exception as e:
        print('connection Failed-',e)

    return connection

try: #mysql and mongodb connection
    conn = create_mysql_connection('localhost', 'root', "Game$321")
    cursor = conn.cursor()

    client = MongoClient("mongodb+srv://prash699:1234@cluster0.ijiil.mongodb.net/?retryWrites=true&w=majority")
    db = client['youtube_thumbnails_and_comments']

    print("MySQL and MongoDB Database connection successful")
except Exception as e:
    print("connection failed-",e)


def upload_thumbnails_and_comments(key,title,thumbnails,video_link,commentors:list,comments:list):
    try:
        image = requests.get(thumbnails).content
        fs = gridfs.GridFS(db)
        fs.put(image, filname=f"{key}-{title}")
        print('thumbnail upload complete')

        coll = db[f'{key}']
        for i in range(len(comments)):
            if video_link in comments[i]:
                coll.insert_one({'title': f"{title}", 'video_link': video_link, 'commentor': commentors[i][1], 'comment': comments[i][1]})
                print(f"{title}",video_link,commentors[i][1],comments[i][1])
        print(f"all comments related to channel {key} - video {title} have been uploaded")
    except Exception as e:
        print("unable to upload thumbnail and comments on MongoDB.-",e)



def create_table():
    try:
        cursor.execute('create database if not exists YouTube_Scraping')
        cursor.execute('use YouTube_Scraping')
        cursor.execute("create table if not exists YouTube_Channels(YouTuber varchar(30) primary key unique)")
        cursor.execute("create table if not exists all_channel_details(youtuber varchar(30),title varchar(200), likes varchar(50),no_of_comments int,video_link varchar(100) primary key unique,thumbnail_link varchar(200))")

    except Exception as e:
        print(e)


def execute_query(query):
    try:
        cursor.execute(query)
        conn.commit()
        print('Youtube video data has been inserted into tables.')
    except Exception as e:
        print('data not inserted-',e)



def upload_video_to_s3(key,videolink,title):
    try:
        target_folder = os.path.join('./youtubevideos')
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        video = YouTube(videolink)
        spcl_char = """|/\:*?"<>"""
        newtitle = title
        for i in title:
            if i in spcl_char:
                newtitle = newtitle.replace(i, "")

        newtitle = os.path.join(target_folder, newtitle)

        streams = video.streams.filter(res='360p').first()
        streams.download(filename=newtitle+".mp4")
        print(f'video {key}-{title} has downloaded into the system.')

        s3 = boto3.resource(
            service_name='s3',
            region_name='ap-south-1',
            aws_access_key_id='AKIAZPZH5ZL44WQG75KG',
            aws_secret_access_key='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')

        s3.Bucket('youtubescrapedvideos').upload_file(Filename=newtitle+".mp4", Key=title+".mp4")
        print(f'video {key}-{title} has uploaded to the S3 bucket.')
    except Exception as e:
        print("unable to upload youtube video on s3 bucket-",e)




for key in keyword: #looping through the youtube channels
    create_table()
    all_details(videospage=search_kw(keywords=key),max_link=max_link)
    query2 = f"insert into YouTube_Channels values('{key}') on duplicate key update YouTuber='{key}' "
    execute_query(query2)


driver.quit()


