#!/usr/bin/env python3

import copy
import csv
import datetime
import pytz
import re
    
def update_top(current_dict, new_value, current_boundary):
    if len(current_dict) < 5 : 
        current_dict[new_value[0]] = new_value[1]
        if current_boundary == 0 :
            current_boundary = new_value[1]
        return min(current_boundary, new_value[1])
    elif current_boundary < new_value[1]: 
        is_deleted=False
        current_dict[new_value[0]] = new_value[1]
        loop_dir=copy.deepcopy(current_dict)        
        for alert in loop_dir.items(): 
            if alert[1] == current_boundary :
                if is_deleted == False :
                    del current_dict[alert[0]]
                    is_deleted=True
                else : 
                    return current_boundary
        return min(current_dict.values())
    else : 
        return current_boundary

def display_top_alerts(alert_list): 
    working_list = copy.deepcopy(alert_list)
    for iteration in range(len(alert_list)): 
        max_count = 0
        top_alert = ""
        for alert in working_list.items(): 
            if alert[1] > max_count : 
                max_count = alert[1]
                top_alert = alert[0]
        del working_list[top_alert]
        print('Alert : {:s} - Occurences : {:d}'.format(top_alert, max_count))  

def import_csv(filename, alerts_list, shift_busyness):
    utc=pytz.UTC

    start_window = utc.localize(datetime.datetime.max)
    end_window = utc.localize(datetime.datetime.min)
    
    with open(filename) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in csvreader:
            # We skip the 1st line which is the title of the different CSV columns
            if row[1] == "incident_number":
                continue
            if row[8] != "" and (datetime.datetime.fromisoformat(row[8]) > end_window ) :
                end_window = datetime.datetime.fromisoformat(row[8])
                # print(f"New end window : {end_window}")
            if (datetime.datetime.fromisoformat(row[7]) < start_window ) :
                start_window = datetime.datetime.fromisoformat(row[7])
                # print(f"New start window : {start_window}")
        
    with open(filename) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')    
        for row in csvreader:
            if row[22] == 'high':
                # Formatting the description to ignore any variable pieces (list cluster name or occurence count)
                description = re.sub("ERROR \(\d+\)", "", re.sub("CRITICAL \(\d+\)", "", row[2]))
                if row[4] == "prod-deadmanssnitch" : 
                    description = "Cluster Has Gone Missing"
                elif row[4] == "app-sre-alertmanager" : 
                    description = "Cluster Provisioning Delay"
                elif row[4] == "Zabbix Service" : 
                    # As v3 is being decoed and not invested on, ignoring those for the statistics
                    continue
                
                if description in alerts_list.keys(): 
                    alerts_list[description] = alerts_list[description] + 1 
                else : 
                    alerts_list[description] = 1
                
                # Formatting the service name (and building it in case it in only in the description)
                service = ""
                if row[4] == "prod-deadmanssnitch" : 
                    # For DMS, we have the cluster name at the begining of the description
                    service_split = '.'.join(re.sub(" has gone missing", "", row[2]).split('.')[0:2])
                    service = re.sub( '\s+', "", re.sub("\[[\w*\s*]+\]", "", service_split))
                elif row[4] == "app-sre-alertmanager" : 
                    # Cluster provisioning delay alerts have the clustername in the middle of the description 
                    service = row[2].split('(')[1].split(' ')[0]
                else :
                    # To align on service name, we only keep the cluster-name+unique ID of the cluster URL
                    service = re.sub("osd-", "", '.'.join(row[4].split('.')[0:2]))
                 
                # row[7] is the 'created_on' field from the CSV
                start_time = datetime.datetime.fromisoformat(row[7])
                if row[8] != "" : 
                    end_time = datetime.datetime.fromisoformat(row[8])
                else :
                    end_time = end_window
                
                feed_shift_busyness(shift_busyness, service, start_time)
                

def get_shift(date_time):
    if date_time.hour < 3 or (date_time.hour == 3 and date_time.minute < 30 ) : 
        return "APAC 1"
    elif date_time.hour < 8 or (date_time.hour == 8 and date_time.minute < 30 ) : 
        return "APAC 2"
    elif date_time.hour < 13 or (date_time.hour == 13 and date_time.minute < 30 ) : 
        return "EMEA"
    elif date_time.hour < 18 : 
        return "NASA 1"
    elif date_time.hour < 22 or (date_time.hour == 22 and date_time.minute < 30 ) : 
        return "NASA 2"
    else :
        # End of UTC day is covered by APAC 1 (next working day)
        return "APAC 1"

def feed_shift_busyness(shift_busyness, service, date_time):
    if date_time.date() not in shift_busyness.keys(): 
        shift_busyness[date_time.date()] = { "APAC 1" : {}, "APAC 2" : {}, "EMEA" : {}, "NASA 1" : {}, "NASA 2" : {}}
    
    shift_incidents = shift_busyness[date_time.date()][get_shift(date_time)]
    if service not in shift_incidents.keys() : 
        shift_incidents[service] = 0
    
    shift_incidents[service] += 1

def display_busyness(shift_busyness): 
    max_alerting_cluster, sum_alerting_clusters = { "APAC 1" : 0, "APAC 2" : 0, "EMEA" : 0, "NASA 1" : 0, "NASA 2" : 0}, { "APAC 1" : 0, "APAC 2" : 0, "EMEA" : 0, "NASA 1" : 0, "NASA 2" : 0}
    
    for date in shift_busyness.keys() : 
        for zone in shift_busyness[date] :
            if len(shift_busyness[date][zone]) > max_alerting_cluster[zone] : 
                max_alerting_cluster[zone] = len(shift_busyness[date][zone])
            sum_alerting_clusters[zone] += len(shift_busyness[date][zone])
    
    for zone in sum_alerting_clusters.keys() : 
        average_alerting_cluster = sum_alerting_clusters[zone] / len(shift_busyness)
        print(f"{zone} : Max number of alerting clusters : {max_alerting_cluster[zone]} - Average number of alerting clusters in a shift : {round(average_alerting_cluster, 1)}")

def time_to_timeslot(time) : 
    minute_slot = 0
    
    if time.minute < 15 : 
        minute_slot = 1
    elif time.minute < 30 : 
        minute_slot = 2
    elif time.minute < 45 : 
        minute_slot = 3
    else :
        minute_slot = 4
        
    return time.hour*4+minute_slot

if __name__ == "__main__":
    alerts_list = {}
    top_alerts = {}
    current_boundary = 0
    shift_busyness = {}
    
    import_csv("test/test_december.csv", alerts_list, shift_busyness)

    for alert in alerts_list.items():
        current_boundary = update_top(top_alerts, alert, current_boundary)
    
    print('## Top alerts')
    display_top_alerts(top_alerts)
    
    print('## Shift Busyness')
    display_busyness(shift_busyness)
    
    print('## Max number of parallel alerting clusters')
