import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def get_temporal(k):
    post_df=pd.read_csv("../../data/processed/posts.csv")
    user_df=pd.read_csv("../../data/processed/users.csv")
    post_df.dropna(subset=['CreationDate'],inplace=True)

    post_df=post_df[post_df.PostTypeId<=2]
    post_df.drop(columns=['LastEditorUserId','LastEditDate','LastActivityDate','Id'],inplace=True)
    post_df.dropna(subset=['CreationDate','OwnerUserId'],inplace=True)
    post_df[['OwnerUserId']]=post_df[['OwnerUserId']].astype(int)
    post_df=post_df[post_df['OwnerUserId']!=-1].reset_index(drop=True)
    user_df=user_df[user_df['Id']!=-1].reset_index(drop=True)

    post_oid_cd_df=post_df[['OwnerUserId','CreationDate']].copy()
    user_oid_cd_df=user_df[['Id','CreationDate']].copy()
    user_oid_cd_df.rename(mapper={'Id':'OwnerUserId'},axis=1,inplace=True)
    final_df=post_oid_cd_df.append(user_oid_cd_df, ignore_index=True)

    final_df.dropna(how='any',inplace=True)
    final_df=final_df.groupby('OwnerUserId').filter(lambda x:x['CreationDate'].count()>=2)
    final_df['CreationDate']=pd.to_datetime(final_df['CreationDate'])
    final_df['diff'] = final_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId')['CreationDate'].diff()
    final_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True],inplace=True)

    df=final_df.copy()
    df.reset_index(drop=True,inplace=True)
    df.dropna(how='any',inplace=True)

    lik=[i for i in range(k)]
    k_df=df.groupby('OwnerUserId').filter(lambda x:x['CreationDate'].count()>=k)
    k_df=k_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId').nth(lik).reset_index()
    k_df['diff']=k_df['diff']/np.timedelta64(1, 'm')

    fin_df=k_df[['OwnerUserId','diff']].copy().reset_index(drop=True)

    x=fin_df.sort_values(["OwnerUserId","diff"]).groupby('OwnerUserId')['diff'].apply(lambda df: df.reset_index(drop=True)).unstack()
    
    return x.reset_index()