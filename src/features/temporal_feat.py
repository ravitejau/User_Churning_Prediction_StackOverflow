import pandas as pd
import numpy as np
from datetime import timedelta

def getPostsInfo(T):
    user_df = pd.read_csv("../../data/processed/users.csv")
    user_df.drop(columns=['AccountId','Reputation','Views','UpVotes','DownVotes','LastAccessDate'],inplace=True)
    user_df.dropna(inplace=True)
    user_df['CreationDate'] = pd.to_datetime(user_df['CreationDate'])
    user_df["EndDate"] = user_df["CreationDate"] + timedelta(days=T)
    user_df['nCreationDate'] = pd.to_timedelta(user_df['CreationDate']).astype('timedelta64[m]').astype(int)
    user_df['nEndDate'] = pd.to_timedelta(user_df['EndDate']).astype('timedelta64[m]').astype(int)
    user_df=user_df[user_df['Id']!=-1].reset_index(drop=True)
    #user_df['Id'] = user_df['Id'].astype(np.int64)
    #user_df.rename(columns={'Id':'UserId'},inplace=True)

    post_df = pd.read_csv("../../data/processed/posts.csv")
    post_df.drop(columns=['PostTypeId','Score','LastEditorUserId','LastEditDate','LastActivityDate','CommentCount'],inplace=True)
    post_df.dropna(inplace=True)
    post_df['CreationDate'] = pd.to_datetime(post_df['CreationDate'])
    post_df['nCreationDate'] = pd.to_timedelta(post_df['CreationDate']).astype('timedelta64[m]').astype(int)
    post_df['OwnerUserId'] = post_df['OwnerUserId'].astype(int)
    post_df=post_df[post_df['OwnerUserId']!=-1].reset_index(drop=True)
    #post_df.rename(columns={'OwnerUserId':'UserId'},inplace=True)

    user_tempFeat = []
    for index,row in user_df.iterrows():
        userId = row['Id']
        startTime = row['nCreationDate']
        endTime = row['nEndDate']
        new_df = post_df[(post_df.OwnerUserId == userId) & (post_df.nCreationDate <= endTime)]
        new_df = new_df.sort_values(by=['nCreationDate'])
        n = len(new_df.index)
        if n > 0:
            if n == 1:
                time = new_df['nCreationDate'].iloc[0]
                diff = time - startTime
                lg = diff
                tsp = endTime - time
                mg = diff/n
            else:
                firstTime = new_df['nCreationDate'].iloc[0]
                diff1 = firstTime - startTime
                new_df['diff'] = new_df['nCreationDate'].diff()
                new_df['diff'] .iloc[0]= diff1
                lg = new_df['diff'].iloc[-1]
                tsp = endTime - new_df['nCreationDate'].iloc[-1]
                mg = new_df['diff'].sum(axis=0)/n
            
            user_tempFeat.append(
            {'userid' : userId,'last_gap':lg,'time_since_last_post':tsp,'mean_gap':mg}) 
    user_tempFeatT = pd.DataFrame(user_tempFeat)

    return user_tempFeatT    
   