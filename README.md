## Data_Lake_Spark
#### Project summary:


In this project, We will use Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. We will deploy this Spark process on a cluster using AWS.


# Project: Data Lake

Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


Project Description

In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.


Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

PROJECT TABLES :

songplays:

        songplay_id       
        start_time         
        user_id            
        level               
        song_id          
        artist_id         
        session_id          
        location            
        user_agent         
 
 --------------------------------------------------
 
 
 users:
 
        user_id            
        first_name         
        last_name         
        gender              
        level              
    
_______________________________________________
    
songs :

        song_id        
        title              
        artist_id          
        year               
        duration            

------------------------------------------------

artists:

        artist_id          
        name                
        location        
        latitude          
        longitude         



------------------------------------------------


time :

        start_time          
        hour               
        day                
        week                
        month              
        year              
        weekday             
 
 
 -------------------------------------------------
 
 
 project steps :
 
 1-create a spark session then save the instance in a variable called spark
 
 2-the path to where the datasets is stored is saved in a variable called "input_data"
 
 3- the desired loaction for the data to be saved within is in the variable called " output_data"
 
 4- the method process_song_data() is called with the parameters "input_data , output_data ,spark" this method will process the song data set and makes the tables [songs_table] & [artist_table]
 
 
5-Then the method process_log_data() is called with the parameters "input_data , output_data ,spark"  this method will process the log data set and makes the tables [users_table] & [time_table] & [songplays_table] 


## END OF PROJECT .
