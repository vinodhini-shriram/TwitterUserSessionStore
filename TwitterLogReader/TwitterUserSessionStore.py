"""
Twitter User Session Store

This script allows the user to read a log file of format - user_id,timestamp,action to generate average time spent by an
user in the application.

It is assumed that the files are processed at day level and the open event on day 1 may have closing event on day 2,
provided that is the last event for that user in the given day. If we are missing a close event for any user, but we
receive another open event, the caller of the method has flexibility to either ignore the first open event or consider
the second open event to be the close event for the first open event.

This script does not require any additional libraries and can run on a python environment(python 2 and python3).

This file can also be imported as a class "TwitterUserSessionStore"

"""
import sys
from collections import defaultdict


class TwitterUserSessionStore:
    """
    Class used to generate user sessions in order to calculate the average time spent by an user in the application.

    Attributes:
    ----------
    log_file_location : str
        The location of the input file
    incomplete_session_file : str
        File where all the incomplete sessions from last run will be located.
    user_average : dict
        Dictionary to keep track of the average duration, everytime we encounter a closing event
        key: user_id; value: incrementing average
    open_sessions: dict
        Dictionary to keep track of all the open sessions for every user.
        key: user_id; value: starting timestamp of the session
    user_session_counter: defaultdict(int)
        Default dictionary where the default value of any user will be 0
        key: user_id; value: number of sessions for the particular user (used in calculating the incremental average)
    """

    def __init__(self, input_file_name):
        self.log_file_location = input_file_name
        self.incomplete_session_file = "incomplete_session_files/incomplete_sessions.txt"

        self.user_average = {}
        self.open_sessions = {}
        self.user_session_counter = defaultdict(int)

    def load_incomplete_sessions(self):
        """
        Loads the open events written in the incomplete_session_file to the open_sessions dict, so that we can process them.
        :return: None
        """
        for log_line in open(self.incomplete_session_file):
            user_id, timestamp, action = log_line.strip().split(",")
            if action == "open":
                self.open_sessions[user_id] = timestamp

    def get_incr_average(self, user_id, duration):
        """
        Calculates the incrementing average for every user, everytime we close a session. Uses formula specified in
        https://math.stackexchange.com/a/106720
        :param user_id: Id of the user as mentioned in the logfile
        :param duration: Calculated duration.
        :return: Incremental average (int)
        """
        prev_average = self.user_average.get(user_id, 0)
        session_count = self.user_session_counter.get(user_id, 1)

        incr_average = prev_average + ((duration - prev_average)/session_count)
        return incr_average

    def end_session(self, user_id, start_timestamp, end_timestamp):
        """
        Ends the current session by doing the following:
        1. calculates duration
        2. updates the user_session_counter, user_average and open_sessions.
        :param user_id: Id of the user as mentioned in the logfile
        :param start_timestamp: Start timestamp of the session
        :param end_timestamp: End timestamp of the session
        :return: None
        """
        duration = int(end_timestamp) - int(start_timestamp)
        self.user_session_counter[user_id] += 1
        self.user_average[user_id] = self.get_incr_average(user_id, duration)
        del self.open_sessions[user_id]

    def process_log_file(self, consider_missing_close=True, read_incomplete_session_file=False,
                         write_incomplete_session_file=False):
        """
        Processes the log file line by line so that only one line is taken into memory at a time. This can be changed to
        reading the file in chunks and process.
        :param consider_missing_close: this parameter when set true considers the next open as a closing event to an
        existing open session for the user when a closing event is missing. If this parameter is set to false, the open
        event without a closing event will be ignored.
        :param read_incomplete_session_file: When set true, this parameter enables us to read the incomplete session file.
        :param write_incomplete_session_file: When set true, this parameter generates the incomplete session file with
        the entries in the open_session.
        :return: None
        """

        if read_incomplete_session_file:
            self.load_incomplete_sessions()

        for log_line in open(self.log_file_location):
            user_id, timestamp, action = log_line.strip().split(",")

            if action == "open":
                # If action is open, we need to know if there is no open sessions already present
                if user_id in self.open_sessions:
                    if consider_missing_close:
                        # If this flag is set to true, we consider next open event as the closing event,
                        # when we didnt get a close event
                        start_timestamp = self.open_sessions.get(user_id, -1)
                        self.end_session(user_id, start_timestamp, timestamp)
                self.open_sessions[user_id] = timestamp

            if action == "close":
                start_timestamp = self.open_sessions.get(user_id, -1)

                if start_timestamp == -1:
                    # If we dont find an already open session for this close,
                    # we dont have any choice other than to ignore it.
                    print("No open session found for: " + log_line)
                else:
                    self.end_session(user_id, start_timestamp, timestamp)


        # Assuming that we will get close events for these open in next day's/hour's log file.
        if write_incomplete_session_file:
            self.write_incomplete_sessions()

    def write_incomplete_sessions(self):
        """
        Writes the remaining incomplete sessions into a file.
        :return: None
        """
        with open(self.incomplete_session_file, 'w') as fp:
            for user_id, timestamp in self.open_sessions.items():
                fp.write("%s,%s,%s" %(user_id, timestamp, "open"))


if __name__ == '__main__':
    input_log_file = sys.argv[1]

    # Considering the next open event as closing event for the missing close session
    ss = TwitterUserSessionStore(input_log_file)
    ss.process_log_file()
    print(ss.user_average)

    # Ignoring open event without a closing event and writing incomplete session
    ss1 = TwitterUserSessionStore(input_log_file)
    ss1.process_log_file(consider_missing_close=False, write_incomplete_session_file=True)
    print(ss1.user_average)

    # Reading incomplete session and closing it from the next logfile
    ss2 = TwitterUserSessionStore("log_files/SecondDayLog.txt")
    ss2.process_log_file(read_incomplete_session_file=True)
    print(ss2.user_average)



