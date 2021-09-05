# Twitter App Average Duration

This project is to calcuate average duration spent by an user in the app.

## Data Sets:
We have log files with the following format.
user_id, timestamp, action.
where action is one of two possible values : [open, close]

## Terminologies:
**Incomplete Sessions**: A log entry which is open for a user and doesnt find its matching close, 
provided that this open log entry is the last entry for that user in the logfile. 

Example: User 1 has a open event and doesnt have any event after that in this particular log file. 
We assume that to be an incomplete session which will be completed in the next log file. 

**Stray Events**: Any open or close event without its matching event is called a stray event. 

Example: If we have a close event for an user but no open event for it, then that event will be termed as stray event.

## Assumptions and Decisions:
1. Only Open events can form incomplete sessions. 
2. Assuming we get these log files as a daily or hourly basis, we are handling our incomplete sessions. 
We write the incomplete open events to a file and process them in the beginning before reading next log file.
3. The logic to handle stray events are decided through arguments sent to the function calls.


## Code Base

- **TwitterUserSessionStore.py**
   This pyspark script is used to read the log file and generate average time spent by the user in the application.  
   Usage: **python TwitterUserSessionStore.py <input_file_name>**