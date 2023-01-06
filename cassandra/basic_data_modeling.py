# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
import cassandra
from cassandra.cluster import Cluster

# %% [markdown]
# ## Create a connection to the Cassandra database

# %%
try:
    cluster = Cluster(['172.18.0.2']) # provide contact points and port
    session = cluster.connect()
except Exception as e:
    print(e)

# %% [markdown]
# ## Create a keyspace, a collection of tables to work in

# %%
try:
    query = """
        
                    CREATE KEYSPACE IF NOT EXISTS musicshop
                    WITH REPLICATION = {'class': 'SimpleStrategy',
                    'replication_factor': 1}
                    """
    
    session.execute(query)
except Exception as e:
    print(e)
finally:
    pass

# %% [markdown]
# ## Connect to the keyspace

# %%
try:
    session.set_keyspace('musicshop')
except Exception as e:
    print(e)

# %% [markdown]
# ### Let's imagine we would like to start creating a Music Library of albums. 
#
# ### We want to ask 3 questions of the data
# #### 1. Give every album in the music library that was released in a given year
# `select * from music_library WHERE YEAR=1970`
# #### 2. Give every album in the music library that was created by a given artist  
# `select * from artist_library WHERE artist_name="The Beatles"`
# #### 3. Give all the information from the music library about a given album
# `select * from album_library WHERE album_name="Close To You"`

# %%
#TODO: Create the normalized version of the table
#TODO: Create the conceptual and logical model of the data in relational DB
#TODO: Create the logical and physical model of the table in Cassandra
query_table_year= """
                CREATE TABLE IF NOT EXISTS musicshop.album_by_year (year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name));

"""

query_table_artist = """
                        CREATE TABLE IF NOT EXISTS musicshop.album_by_artists (artist_name text, album_name text, year int, PRIMARY KEY (artist_name, album_name));
"""

query_table_album = """
                    CREATE TABLE IF NOT EXISTS musicshop.music_by_album (album_name text, artist_name text, year int, PRIMARY KEY (album_name, artist_name));
"""

try:
    session.execute(query_table_year)
    # session.execute(query_table_artist)
except Exception as e:
    print(e)
   
try:
    # session.execute(query_table_album)
    session.execute(query_table_artist)
except Exception as e:
    print(e)

try:
    # session.execute(query_table_album)
    session.execute(query_table_album)
except Exception as e:
    print(e)

# %%
# query = "INSERT INTO music_library (#####)"
query = "INSERT INTO album_by_year (year, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s)"

# query1 = "INSERT INTO artist_library (#####)"
query1 = "INSERT INTO album_by_artists ( artist_name, year, album_name)"
query1 = query1 + " VALUES (%s, %s, %s)"

# query2 = "INSERT INTO album_library (#####)"
query2 = "INSERT INTO music_by_album ( album_name, artist_name, year)"
query2 = query2 + " VALUES (%s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Who", "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Beatles", 1970, "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Beatles", 1965, "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Who", 1965, "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Monkees", 1966, "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Carpenters", 1970, "Close To You"))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("Let it Be", "The Beatles", 1970))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("Rubber Soul", "The Beatles", 1965))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("My Generation", "The Who", 1965))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("The Monkees", "The Monkees", 1966))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("Close To You", "The Carpenters", 1970))
except Exception as e:
    print(e)

# %% [markdown]
# This might have felt unnatural to insert duplicate data into the tables. If I just normalized these tables, I wouldn't have to have extra copies! While this is true, remember there are no `JOINS` in Apache Cassandra. For the benefit of high availibity and scalabity, denormalization must be how this is done. 

# %% [markdown]
# ## Validate the  data model

# %%
query = "select * from album_by_year WHERE year = 1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name)

# %%
query = "select * from album_by_artists WHERE artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name, row.album_name, row.year)

# %%
session.shutdown()
cluster.shutdown()

# %%
