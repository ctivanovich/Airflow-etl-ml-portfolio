import numpy as np
import pandas as pd

def find_match_agreement(recs_df_uri, k):
    recs_df = pd.read_csv(
        recs_df_uri,
        names = ['user_id', 'target_user_id']
    )
    matches = {}
    for user_grp in recs_df.groupby('user_id'):
        user = user_grp[0]
        matches[user] = []
        user_recs = user_grp[1].target_user_id
        target_recs = recs_df[(recs_df.user_id.isin(user_recs))][['user_id', 'target_user_id']]
        for tuser_grp in target_recs.groupby('user_id'):
            if user in tuser_grp[1].target_user_id.values:
                matches[user].append(tuser_grp[0])
                if len(matches[user]) == k:
                    break
        if not matches[user]:
            matches.pop(user)
    return recs_df, matches

def insert_and_recreate_index(prior_task, **context):
    recs_df, matches = context['task_instance'].xcom_pull(task_ids=prior_task)
    all_new_indices = []
    for user, matched in matches.items():
        rec_indices = recs_df[recs_df.user_id == user].index
        matched_indices = recs_df[(recs_df.user_id == user) & (recs_df.target_user_id.isin(matched))].index
        cleaned_indices = np.delete(rec_indices, np.where(rec_indices.isin(matched_indices)))
        new_indices = np.concatenate((matched_indices, cleaned_indices))
        all_new_indices.append(new_indices)
    return recs_df, np.concatenate(all_new_indices)

def reindex_and_deliver_df(recs_df_uri, prior_task, **context):
    recs_df, new_indices = context['task_instance'].xcom_pull(task_ids=prior_task)
    recs_df.reindex(new_indices).to_csv(recs_df_uri, index = False, header = False)