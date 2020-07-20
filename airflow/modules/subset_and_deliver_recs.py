import numpy as np
import pandas as pd

from functools import reduce
from itertools import combinations
from google.cloud import storage


def send_collated_recs_to_unmodified_location(df, uri_destination):
    df.to_csv(uri_destination, index = None, header = None)

def construct_recs_df(name, uri_base):
    df = []
    for district_id in range(1,14):
        try:
            ### temporary solution due to rating in source implict csv files
            if name == 'implicit':
                df_temp = pd.read_csv(
                    uri_base.format(district_id), 
                    usecols = ['user_id', 'target_user_id'],
                    names = ['user_id', 'target_user_id', 'rating']
                    )
            else:
                df_temp = pd.read_csv(
                    uri_base.format(district_id), 
                    names = ['user_id', 'target_user_id']
                )
            df.append(df_temp)
        except FileNotFoundError:
            continue
    df = pd.concat(df)
    df.name = name
    return df

def get_shared_users(frames):
    """This assumes a minimum of two dataframes, and returns users common to all frames as a list"""
    assert len(frames) >= 2
    
    all_users_list = [df.user_id.unique() for df in frames.values()]
    
    duplicate_users = reduce(set.intersection, map(set, list(all_users_list)))
    
    return list(duplicate_users)

def get_pairwise_shared_users(frames, fully_shared_users):
    '''Takes in our name:df dict, and outputs the set Cartersian product of each pair of frames 
    in a dictionary, with a tuple of the dataframe names as the key, e.g. ('rec1', 'rec2'): shared_user_list.
    
    In the event that there are no shared users between methods, the resultant set will be empty.
    '''
    shared = {}
    name_pairs = list(combinations(frames.keys(), 2))
    for pair in name_pairs:
        temp_shared = get_shared_users({name:frame for name, frame in frames.items() if name in pair})
        ### temp_shared may contain a small slice of users from methods outside this pair, so we subset
        ### on the set of users shared between all methods
        shared[pair] = list(set(temp_shared) - set(fully_shared_users))
        
    return shared

def randomize_and_separate_shared_users(shared_users, frames):
    shared_users = np.random.permutation(list(shared_users))
    separated_collection = {}
    n_dfs = len(frames)
    for i, name in enumerate(frames):
        start = i * len(shared_users)//n_dfs
        stop = start + len(shared_users)//n_dfs
        separated_collection[name] = shared_users[start:stop]
    return separated_collection
    
def main_subsetter(rec_methods_map, tab_destinations):
    frames = {}

    for name, rec_method_base in rec_methods_map.items():
        df_temp = construct_recs_df(name, rec_method_base)
        frames[name] = df_temp
    del df_temp
    ### each frame in frames has a name attribute
    
    ### here we take advantage of that to send these collated, unfiltered recs
    for name, tab_destination in tab_destinations.items():
        send_collated_recs_to_unmodified_location(frames[name], tab_destination)
    
    ### now we begin filtering
    fully_shared_users = get_shared_users(frames)
    ### a set of users shared b/t all methods; if only 2 methods, we can stop searching here
    ### and move on to sorting recs output
    randomized_and_split_fully_shared_users = randomize_and_separate_shared_users(fully_shared_users, frames)
    
    for name in frames:
        for set_name, split_users in randomized_and_split_fully_shared_users.items():
            if name == set_name: 
                ### these are the users we want to remain, so we do nothing
                continue
            else:
                ### we remove recs if the users for this frame are in another method's list
                frames[name] = frames[name][~frames[name]['user_id'].isin(split_users)]
            
    pairwise_shared_users = get_pairwise_shared_users(frames, fully_shared_users) if len(frames) > 2 else None
    ### a dict with items as {(df1.name, df2.name):shared_users_set}
    
    if pairwise_shared_users:
        for pair, shared in pairwise_shared_users.items():
            these_frames = {name:frame for name, frame in frames.items() if name in pair}
            separated = randomize_and_separate_shared_users(shared, these_frames)
            frames[pair[0]] = frames[pair[0]][~frames[pair[0]].user_id.isin(separated[pair[1]])]
            frames[pair[1]] = frames[pair[1]][~frames[pair[1]].user_id.isin(separated[pair[0]])]
    
    return frames

def subset_and_deliver_daily_recommendations(rec_methods_map, honjitsu_destinations, tab_destinations):
    subset_frames = main_subsetter(rec_methods_map, tab_destinations)
    for name, honjitsu_destination in honjitsu_destinations.items():
        subset_frames[name].to_csv(honjitsu_destination, index = False, header = False)