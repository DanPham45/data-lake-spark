# Project Summary

This project is to extract data from storage (S3), create a database schema and store the data with the new schema back to S3.

## Usage

Open commandline and run the following script to perform the ETL job.
```bash
python etl.py
```
## Other files in the repository
There are 6 files in the workspace:

* dl.cfg: stores the aws credentials
* etl.py: performs ETL job by extracting, processing files from data source and loading them into S3 folders
* README.md: explanation on the project
 
 ## Database design
 This project creates 5 folders in S3 bucket:
     
* **songplays** (fact table) - records in log data associated with song plays i.e. records with page NextSong. Columns are: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* **users** - users in the app. Columns are: user_id, first_name, last_name, gender, level
* **songs** - songs in music database. Columns are: song_id, title, artist_id, year, duration
* **artists** - artists in music database. Columns are: artist_id, name, location, latitude, longitude
* **time** - timestamps of records in songplays broken down into specific units. Columns are: start_time, hour, day, week, month, year, weekday

 ## ETL Process
After the database and tables were created, the ETL process can start. It would go through 2 separate processes:
* process_song_file: extracts data from song_data s3 folder, inserts data into song table and artist table, saves back to s3
* process_log_file: extracts data from log_data s3 folder, inserts data into time, users and songplays table, saves back to s3
 