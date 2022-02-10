#!/usr/bin/env python
#----------------------------------------------------------------------------
# Created By  : Tugberk Ozdemir
# Created Date: 10.02.2022
# version ='1.0'
# status = 'Development'
# ---------------------------------------------------------------------------
# This code detects the multiple authorization attempts with various outcomes
# in specified time interval listed in a logfile.
# ---------------------------------------------------------------------------


import pandas as pd
import json
import configparser


# This section parses required parameters from external configuration file
configuration_file = 'conf.ini'
config = configparser.ConfigParser()
config.read(configuration_file)

interval_hours = config.getint('DEFAULT', 'interval')
interval_seconds = interval_hours * 3600

atLeast = config.getint('DEFAULT', 'atLeast')


# Read the log file line by line
logfile = 'auth.log'
df = []
for line in open(logfile, 'r'):
    df.append(json.loads(line))


# Normalize dataframe
df = pd.json_normalize(df)

# Mind the timestamp, otherwise python assumes it's a string.
df['timestamp'] = pd.to_datetime(df['timestamp'])


#
# This function detects the users triggered NT_STATUS_OK incidents occured more
# than specified numbers in specified time interval and writes them into a file.
#
def status_ok_incidents():
    status_ok_people = df[df['Authentication.status'] == "NT_STATUS_OK"]

    filtered_status_ok_people = status_ok_people.groupby('Authentication.clientAccount')['timestamp'].diff().dt.seconds.lt(interval_seconds).groupby(status_ok_people['Authentication.clientAccount']).sum().reset_index(name='times')

    filtered_status_ok_people = filtered_status_ok_people[filtered_status_ok_people.times > atLeast]

    filtered_status_ok_people.to_csv('status_ok_incidents.csv', sep='\t')



#
# This function detects the users triggered NT_STATUS_WRONG_PASSWORD incidents
# occured more than specified numbers in specified time interval and writes them into a file.
#
def wrong_password_incidents():
    wrong_password_people = df[df['Authentication.status'] == "NT_STATUS_WRONG_PASSWORD"]

    filtered_wrong_password_people = wrong_password_people.groupby('Authentication.clientAccount')['timestamp'].diff().dt.seconds.lt(interval_seconds).groupby(wrong_password_people['Authentication.clientAccount']).sum().reset_index(name='times')

    filtered_wrong_password_people = filtered_wrong_password_people[filtered_wrong_password_people.times > atLeast]

    filtered_wrong_password_people.to_csv('wrong_password_incidents.csv', sep='\t')



#
# This function detects the users triggered NT_STATUS_ACCOUNT_DISABLED incidents
# occured more than specified numbers in specified time interval and writes them into a file.
#
def account_disabled_incidents():
    account_disabled_people = df[df['Authentication.status'] == "NT_STATUS_ACCOUNT_DISABLED"]

    filtered_account_disabled_people = account_disabled_people.groupby('Authentication.clientAccount')['timestamp'].diff().dt.seconds.lt(interval_seconds).groupby(account_disabled_people['Authentication.clientAccount']).sum().reset_index(name='times')

    filtered_account_disabled_people = filtered_account_disabled_people[filtered_account_disabled_people.times > atLeast]

    filtered_account_disabled_people.to_csv('account_disabled_incidents.csv', sep='\t')


#
# This function detects the users triggered NT_STATUS_NO_SUCH_USER incidents
# occured more than specified numbers in specified time interval and writes them into a file.
#
def no_such_user_incidents():
    no_such_people = df[df['Authentication.status'] == "NT_STATUS_ACCOUNT_DISABLED"]

    filtered_no_such_people = no_such_people.groupby('Authentication.clientAccount')['timestamp'].diff().dt.seconds.lt(interval_seconds).groupby(no_such_people['Authentication.clientAccount']).sum().reset_index(name='times')

    filtered_no_such_people = filtered_no_such_people[filtered_no_such_people.times > atLeast]

    filtered_no_such_people.to_csv('no_such_user_incidents.csv', sep='\t')



# Driver section

account_disabled_incidents()
no_such_user_incidents()
