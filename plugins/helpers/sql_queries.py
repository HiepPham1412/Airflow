class SqlQueries:
    songplay_table_insert = ("""
    INSERT INTO songplay (start_time,
                          user_id,
                          level,
                          song_id,
                          artist_id,
                          session_id,
                          location,
                          user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000* INTERVAL '1 Second' AS start_time, 
            se.userid as user_id, 
            se.level AS level, 
            ss.song_id AS song_id, 
            ss.artist_id AS artist_id, 
            se.sessionid AS session_id, 
            se.location AS location, 
            se.userAgent AS user_agent
     FROM staging_events se
     JOIN staging_songs  ss
     ON (ss.title = se.song AND ss.artist_name = se.artist)
     WHERE  se.page = 'NextSong';
    """)


    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplay
    """)
    