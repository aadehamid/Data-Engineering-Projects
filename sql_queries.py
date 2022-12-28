# DROP TABLES

songplay_table_drop = """
drop table if exists songplays;
"""
user_table_drop = """
drop table if exists users;
"""
song_table_drop = """
drop table if exists songs;
"""
artist_table_drop = """
drop table if exists artists;
"""
time_table_drop = """
drop table if exists time;
"""

# CREATE TABLES
time_table_create = """
create table if not exists time (start_time timestamp not null primary key,
hour INT NOT NULL CHECK (hour >= 0),
day INT NOT NULL CHECK (day >= 0),
week INT NOT NULL CHECK (week >= 0),
month INT NOT NULL CHECK (month >= 0),
year INT NOT NULL CHECK (year >= 0),
weekday varchar not null);
"""

user_table_create = """
create table if not exists users (user_id int not null, first_name varchar, last_name varchar, gender char, level varchar, primary key (user_id))
"""

songplay_table_create = """
create table if not exists songplays (songplay_id serial primary key, start_time timestamp not null,
user_id int not null, level varchar not null, song_id varchar, artist_id varchar, session_id varchar not null, location varchar, user_agent varchar not null);
"""



artist_table_create = """
create table if not exists artists (artist_id varchar, name varchar not null, location varchar , latitude float ,
longtitude float, primary key (artist_id) );
"""

song_table_create = """
create table if not exists songs (song_id varchar primary key, title varchar not null, artist_id varchar, year int, duration float not null);
"""

# INSERT RECORDS
songplay_table_insert = ("""
        insert into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
            values(DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")


user_table_insert = ("""
                     insert into users (user_id, first_name, last_name, gender, level) values(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

song_table_insert = """
insert into songs (song_id, title, artist_id, year, duration) values(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
"""

artist_table_insert = """
insert into artists (artist_id, name, location, latitude ,longtitude ) values(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
"""


time_table_insert = ("""
                     insert into time (start_time, hour, day, week, month, year, weekday) values(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""
               select a.artist_id , s.song_id
               from songs s
               inner join artists a
               on s.artist_id = a.artist_id
               where s.title = %s
                and a.name = %s
                and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [time_table_create, user_table_create, songplay_table_create, artist_table_create, song_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
