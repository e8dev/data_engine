import pandas as pd
from memory_profiler import memory_usage
import psutil
import resource
import sys

from .coordinates_to_country import coordinates_to_country

from ..repositories.events_repository import read_events
from ..repositories.results_repository import save_results

MEMORY_THRESHOLD = 150
MEMORY_MULTIPLIER = 0.1
LIMIT_STEP = 1000

'''
for hard limit
#linux
def memory_limit():
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    print(hard, soft)
    # Convert KiB to bytes, and divide in two to half
    memory_limit_bytes = 100 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (memory_limit_bytes, memory_limit_bytes))

#linux
def get_memory():
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free_memory += int(sline[1])
    return free_memory  # KiB
'''

def process_data(limit):

    all = read_events(limit)

    for events_dataframe in all:
        #print(events_dataframe['coordinates'])
        events_dataframe['country'] = events_dataframe['coordinates'].apply(coordinates_to_country)
        events_dataframe['created_at'] = pd.to_datetime(events_dataframe['created_at'])
        events_dataframe['date'] = events_dataframe['created_at'].dt.date
        events_dataframe = events_dataframe[events_dataframe['event_type'] == 'purchase']
        results = events_dataframe.groupby(['country', 'device_type', 'date']).size().reset_index(name='purchases')

        save_results(results)

    return 

def adjust_memory():
    #move logic here
    pass
    

def process_data_old_school(limit, offset):

    while True:

        events_dataframe = read_events(limit, offset)
        
        if events_dataframe.empty:
            break

        events_dataframe['country'] = events_dataframe['coordinates'].apply(coordinates_to_country)
        events_dataframe['created_at'] = pd.to_datetime(events_dataframe['created_at'])
        events_dataframe['date'] = events_dataframe['created_at'].dt.date
        events_dataframe = events_dataframe[events_dataframe['event_type'] == 'purchase']
        results = events_dataframe.groupby(['country', 'device_type', 'date']).size().reset_index(name='purchases')

        save_results(results)

        #measure memory
        mem_usage = check_memory_mb()

        if mem_usage > MEMORY_THRESHOLD:
            if(limit >LIMIT_STEP):
                limit -= LIMIT_STEP
        else:
            limit += LIMIT_STEP

        # Update offset
        offset += limit

        # Logging memory and limit/offset values
        print("Memory Usage:", mem_usage)
        print("Limit:", limit, "Offset:", offset)


    return 


def check_memory_mb():
    return psutil.Process().memory_info().rss / (1024 * 1024)


#@profile
def run_purchase_count_transformer() -> None:

    # memory limits params

    memory_threshold = 150

    '''
    hard limit setting
    try:
        events_dataframe = read_events(limit, offset)
        process_data(events_dataframe)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)

    '''


    #chunk = 100
    #process_data(chunk)

    process_data_old_school(1000, 0)

    memory_usage_process = check_memory_mb()
    print(memory_usage_process)


    

    








    
