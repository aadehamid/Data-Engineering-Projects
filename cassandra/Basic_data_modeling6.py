#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
#     print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
# print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
    


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# In[5]:


# It is difficult to explore the data in the csv format
# especially if I want to check for the naming convension for the columns
# this code cell convert the csv file to a dataframe, which I will use for the rest of the project

df = pd.read_csv('event_datafile_new.csv')


# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[6]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[7]:


# TO-DO: Create a Keyspace 

query = """
        CREATE KEYSPACE IF NOT EXISTS musicshop WITH REPLICATION = {
        'class':'SimpleStrategy', 'replication_factor':1
        }
"""
try:
    session.execute(query)
except Exception as e:
    print(e)


# #### Set Keyspace

# In[8]:


# Set KEYSPACE to the keyspace specified above
session.set_keyspace('musicshop')


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# ### Create table for query 1

# **Solution for query 1**
# - I used a pandas dataframe to load the data because it is simpler and easy to see what is going on. he csvreade code snippet provided if loading wrong data into the DB
# 
# - The partition key for the data is sessionId
# - The clustering key is itemInSession
# - the columns used in the table are: ('artist', 'itemInSession', 'length', 'sessionId', 'song')
# 

# #### create the table - music_by_sessionID-  for query 1

# In[9]:


query1 = """
        CREATE TABLE music_by_sessionID (sessionId int, itemInSession int, artist text, 
        length float, song text, 
        PRIMARY KEY ((sessionId), itemInSession))
"""

try:
    session.execute(query1)
except Exception as e:
    print(e)


# ####  create a dataframe for the data to insert into music_by_sessionID

# In[10]:


df_music_by_sessionID = df.loc[:, ['sessionId', 'itemInSession', 'artist',
                                    'length', 'song']].copy()

# load the above dataframe into music_by_sessionID Cassandra table
query = """insert into music_by_sessionID 
    (sessionId, itemInSession, artist, length, song) VALUES (%s, %s, %s, %s, %s)"""
for index, row in df_music_by_sessionID.iterrows():
    
    session.execute(query, (row.sessionId, row.itemInSession, row.artist,  row.length, 
                     row.song))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[11]:


query = """
        select artist, song, length from music_by_sessionID 
        where sessionId = 338 and itemInSession = 4

"""

rows = session.execute(query)
for row in rows:
    print(row)


# ### Create table for query 2

# **Solution for query 2**
# - I used a pandas dataframe to load the data because it is simpler and easy to see what is going on. he csvreade code snippet provided if loading wrong data into the DB
# 
# - The partition key for the data is (userId, sessionId)
# - The clustering keys are itemInSession
# - the columns used in the table are: ('artist', 'firstName',  'itemInSession', 'lastName','sessionId', 'song', 'userId')
# - I used WITH CLUSTERING ORDER BY (itemInSession ASC)
# 
# 

# In[12]:


query2 = """
        CREATE TABLE music_by_userId (userId int, sessionId int, itemInSession int, 
        firstName text, lastName text,artist text,  song text,
        PRIMARY KEY ((userId, sessionId), itemInSession)) 
        WITH CLUSTERING ORDER BY (itemInSession DESC)
"""

try:
    session.execute(query2)
except Exception as e:
    print(e)


# ####  create a dataframe for the data to insert into music_by_userId

# In[16]:


table_2_Cols = ['userId', 'sessionId', 'itemInSession', 'firstName',  
'lastName', 'artist', 'song']
df_music_by_userId= df.loc[:, table_2_Cols].copy()

# load the above dataframe into music_by_sessionID Cassandra table
query = """insert into music_by_userId 
    (userId, sessionId, itemInSession, firstName, lastName, artist,song) \
    VALUES (%s, %s, %s, %s, %s, %s, %s)"""

for index, row in df_music_by_userId.iterrows():
    table2_data = (row.userId, row.sessionId, row.itemInSession, 
                   row.firstName, row.lastName, row.artist,  row.song)
    
    session.execute(query, table2_data)
    
    


# #### Do a SELECT to verify that the data have been inserted into each table

# In[19]:


query = """
        select artist, song , firstName, lastName from music_by_userId 
        where userid = 10 and sessionid = 182

"""

rows = session.execute(query)
for row in rows:
    print(row)


# ### Create table for query 3

# **Solution for query 3**
# - I used a pandas dataframe to load the data because it is simpler and easy to see what is going on. he csvreade code snippet provided if loading wrong data into the DB
# 
# - The partition key for the data is song
# - clustering key is userId
# - the columns used in the table are: ('firstName',  'lastName','song')
# 
# 
# 

# In[20]:


query3 = """
        CREATE TABLE music_by_song ( song text, userID int, firstName text, 
       lastName text, PRIMARY KEY ((song), userId))
"""

try:
    session.execute(query3)
except Exception as e:
    print(e)


# ####  create a dataframe for the data to insert into music_by_userId

# In[21]:


table_3_Cols = ['song', 'userId', 'firstName', 'lastName']
df_music_by_song = df.loc[:, table_3_Cols].copy()

# load the above dataframe into music_by_sessionID Cassandra table
query = """insert into music_by_song 
    (song, userId, firstName, lastName) VALUES (%s, %s, %s, %s)"""
for index, row in df_music_by_userId.iterrows():
    
    session.execute(query, (row.song, row.userId, row.firstName, row.lastName))
    


# #### Do a SELECT to verify that the data have been inserted into each table

# In[22]:


query = """
        select firstName, lastName from music_by_song 
        where song = 'All Hands Against His Own'

"""

rows = session.execute(query)
for row in rows:
    print(row)


# ### Drop the tables before closing out the sessions

# In[23]:


## Drop the table before closing out the sessions
try:
    
    session.execute("drop table music_by_userId")
except Exception as e:
    print(e)
    
try:
    session.execute("drop table music_by_sessionID")
except Exception as e:
    print(e)
    
try:
    session.execute("drop table music_by_song")
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[24]:



session.shutdown()
cluster.shutdown()

