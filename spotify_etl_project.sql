// Storage Integration

create or replace storage integration s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = ''
  STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-de')
  COMMENT = 'Creating connection to S3' ;


DESC integration s3_init;

// Song table creation

CREATE OR REPLACE TABLE SPOTIFY_TOP50_GLOBAL.PUBLIC.SONGS_S3_INT(
    SONG_ID VARCHAR(30),
    song_name VARCHAR(1000),
    duration_ms INT,
    url VARCHAR(1000),
    popularity INT,
    song_added DATE,
    album_id VARCHAR(100),
    artist_id VARCHAR(100) 
    );

// Albums table creation
CREATE OR REPLACE TABLE SPOTIFY_TOP50_GLOBAL.PUBLIC.ALBUMS_S3_INT(
    album_id VARCHAR(30),
    name VARCHAR(1000),
    release_date DATE,
    total_tracks INT,
    url VARCHAR(1000)
    );


// Artist table creation
CREATE OR REPLACE TABLE SPOTIFY_TOP50_GLOBAL.PUBLIC.ARTIST_S3_INT(	
    artist_id VARCHAR(30),
    artist_name VARCHAR(1000),
    external_url VARCHAR(1000)
    );

// Creating Schema

CREATE OR REPLACE SCHEMA SPOTIFY_TOP50_GLOBAL.file_formats;

// Create file format object
CREATE OR REPLACE file format SPOTIFY_TOP50_GLOBAL.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

    
 // Create album stage object with integration object & file format object //
CREATE OR REPLACE stage SPOTIFY_TOP50_GLOBAL.external_stages.album_csv_folder
    URL = 's3://spotify-etl-project-de/transformed_data/album_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = SPOTIFY_TOP50_GLOBAL.file_formats.csv_fileformat;

//Load data using copy command
COPY INTO SPOTIFY_TOP50_GLOBAL.PUBLIC.albums_s3_int
    FROM @SPOTIFY_TOP50_GLOBAL.external_stages.album_csv_folder;

// Query loaded data
SELECT * FROM SPOTIFY_TOP50_GLOBAL.PUBLIC.albums_s3_int;

// Create artist stage object with integration object & file format object //
CREATE OR REPLACE stage SPOTIFY_TOP50_GLOBAL.external_stages.artist_csv_folder
    URL = 's3://spotify-etl-project-de/transformed_data/artist_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = SPOTIFY_TOP50_GLOBAL.file_formats.csv_fileformat;

//Load data using copy command

COPY INTO SPOTIFY_TOP50_GLOBAL.PUBLIC.artist_s3_int
    FROM @SPOTIFY_TOP50_GLOBAL.external_stages.artist_csv_folder;

// Query loaded data
select * from spotify_top50_global.public.artist_s3_int;

// Create songs stage object with integration object & file format object //
CREATE OR REPLACE stage SPOTIFY_TOP50_GLOBAL.external_stages.songs_csv_folder
    URL = 's3://spotify-etl-project-de/transformed_data/songs_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = SPOTIFY_TOP50_GLOBAL.file_formats.csv_fileformat;

//Load data using copy command
COPY INTO SPOTIFY_TOP50_GLOBAL.PUBLIC.songs_s3_int
    FROM @SPOTIFY_TOP50_GLOBAL.external_stages.songs_csv_folder;

// Query loaded data
select * from spotify_top50_global.public.songs_s3_int;

// Define Album pipe
CREATE OR REPLACE pipe SPOTIFY_TOP50_GLOBAL.external_stages.album_pipe
auto_ingest = TRUE
AS
COPY INTO SPOTIFY_TOP50_GLOBAL.PUBLIC.albums_s3_int
FROM @SPOTIFY_TOP50_GLOBAL.external_stages.album_csv_folder;  

// Describe pipe get the notification_channel and copy it for S3 event 
DESC pipe SPOTIFY_TOP50_GLOBAL.external_stages.album_pipe;

// Define Artist pipe
CREATE OR REPLACE pipe SPOTIFY_TOP50_GLOBAL.external_stages.artist_pipe
auto_ingest = TRUE
AS
COPY INTO SPOTIFY_TOP50_GLOBAL.PUBLIC.artist_s3_int
FROM @SPOTIFY_TOP50_GLOBAL.external_stages.artist_csv_folder;

// Describe pipe get the notification_channel and copy it for S3 event 
DESC pipe SPOTIFY_TOP50_GLOBAL.external_stages.artist_pipe;

// Define Song pipe
CREATE OR REPLACE pipe SPOTIFY_TOP50_GLOBAL.external_stages.song_pipe
auto_ingest = TRUE
AS
COPY INTO SPOTIFY_TOP50_GLOBAL.PUBLIC.songs_s3_int
FROM @SPOTIFY_TOP50_GLOBAL.external_stages.songs_csv_folder;

// Describe pipe get the notification_channel and copy it for S3 event 
DESC pipe SPOTIFY_TOP50_GLOBAL.external_stages.song_pipe;
