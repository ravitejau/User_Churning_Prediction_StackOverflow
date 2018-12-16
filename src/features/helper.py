import pandas as pd
import numpy as np
from datetime import timedelta

class helper:
    def get_user_id(self,inp):
        
        post_df=pd.read_csv("../../data/processed/posts.csv")
        post_df.drop(columns=['LastEditorUserId','LastEditDate','LastActivityDate'],inplace=True)
        post_df.dropna(subset=['OwnerUserId','CreationDate'],inplace=True)
        post_df[['OwnerUserId']]=post_df[['OwnerUserId']].astype(int)

        post_df=post_df[post_df.PostTypeId<=2]

        
        post_df['CreationDate']=pd.to_datetime(post_df['CreationDate'])
        post_df['diff'] = post_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId')['CreationDate'].diff()
        post_df=post_df.sort_values(['OwnerUserId', 'CreationDate'], ascending=[True, True]).reset_index(drop=True)
        post_df=post_df[post_df['OwnerUserId']!=-1].reset_index(drop=True)
        post_df=post_df.fillna(0)
        post_df['diff']=pd.to_timedelta(post_df['diff'])

        k=inp
        
        days=365/2
        l_k_df=post_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId').nth(k).reset_index()
        c_k_df=l_k_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId').filter(lambda x:x['diff']>=pd.to_timedelta(str(days)+' days'))
        nc_k_df=l_k_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId').filter(lambda x:x['diff']<pd.to_timedelta(str(days)+' days'))

        l_oc_k_df=post_df.groupby('OwnerUserId').filter(lambda x:x['CreationDate'].count()==k)
        oc_k_df=l_oc_k_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId').nth(-1).reset_index()


        Churn_users= c_k_df['OwnerUserId'].tolist()
        Other_churn= oc_k_df['OwnerUserId'].tolist()
        NonChurn_users=nc_k_df['OwnerUserId'].tolist()
        Churn_users.extend(Other_churn)
        print('len of churn',len(Churn_users))
        print('len of Nonchurn',len(NonChurn_users))
        return(Churn_users,NonChurn_users)

    def get_user_id_T(self,T):
        post_df=pd.read_csv("../../data/processed/posts.csv")
        post_df.drop(columns=['LastEditorUserId','LastEditDate','LastActivityDate','PostTypeId','Score','CommentCount'],inplace=True)
        post_df.dropna(inplace=True)
        post_df[['OwnerUserId']]=post_df[['OwnerUserId']].astype(int)

        post_df['CreationDate'] = pd.to_datetime(post_df['CreationDate'])
        post_df['nCreationDate'] = pd.to_timedelta(post_df['CreationDate']).astype('timedelta64[m]').astype(int)
        post_df['diff'] = post_df.sort_values(['OwnerUserId','nCreationDate'], ascending=[True, True]).groupby('OwnerUserId')['nCreationDate'].diff()
        post_df=post_df.sort_values(['OwnerUserId', 'nCreationDate'], ascending=[True, True]).reset_index(drop=True)
        post_df=post_df[post_df['OwnerUserId']!=-1].reset_index(drop=True)

        post_df=post_df.fillna(0)

        T_min = T*24*60
        days = 365/2
        T_days_min = days*24*60

        user_df = pd.read_csv("../../data/processed/users.csv")
        user_df.drop(columns=['AccountId','Reputation','Views','UpVotes','DownVotes','LastAccessDate'],inplace=True)
        user_df.dropna(inplace=True)
        user_df['CreationDate'] = pd.to_datetime(user_df['CreationDate'])
        user_df["EndDate"] = user_df["CreationDate"] + timedelta(days=T)
        user_df["ChurnDate"] = user_df["EndDate"] + timedelta(days=days)
        user_df['nCreationDate'] = pd.to_timedelta(user_df['CreationDate']).astype('timedelta64[m]').astype(int)
        user_df['nEndDate'] = pd.to_timedelta(user_df['EndDate']).astype('timedelta64[m]').astype(int)
        user_df['nChurnDate'] = pd.to_timedelta(user_df['ChurnDate']).astype('timedelta64[m]').astype(int)
        user_df=user_df[user_df['Id']!=-1].reset_index(drop=True)

        churn_users = []
        non_churn_users = []
        for index,row in user_df.iterrows():
            userId = row['Id']
            endTime = row['nEndDate']
            churnTime = row['nChurnDate']
            new_df = post_df[(post_df.OwnerUserId == userId)]
            if len(new_df.index) > 0:
                new_df = new_df[(new_df.nCreationDate > endTime) & (new_df.nCreationDate <= churnTime)]
                if len(new_df.index) > 0:
                    non_churn_users.append(userId)
                else:
                    churn_users.append(userId)
        return(churn_users,non_churn_users)

    def get_posts_df(self, k):

        post_df = pd.read_csv("E:/ASU_CourseWork/Fall_2018/SML/Project/sof_user_churn/data/processed/posts.csv")
        post_df.drop(columns=['LastEditorUserId', 'LastEditDate', 'LastActivityDate'], inplace=True)
        post_df.dropna(subset=['OwnerUserId', 'CreationDate'], inplace=True)
        post_df[['OwnerUserId']] = post_df[['OwnerUserId']].astype(int)
        post_df = post_df[post_df.PostTypeId <= 2]
        post_df['CreationDate'] = pd.to_datetime(post_df['CreationDate'])
        post_df['diff'] = \
        post_df.sort_values(['OwnerUserId', 'CreationDate'], ascending=[True, True]).groupby('OwnerUserId')[
            'CreationDate'].diff()
        post_df = post_df.sort_values(['OwnerUserId', 'CreationDate'], ascending=[True, True]).reset_index(drop=True)
        post_df = post_df[post_df['OwnerUserId'] != -1].reset_index(drop=True)
        post_df = post_df.fillna(0)

        k = 5 + 1
        k_df = post_df.groupby('OwnerUserId').filter(lambda x: x['CreationDate'].count() >= k)

        k_df = k_df.sort_values(["OwnerUserId", "CreationDate"]).reset_index(drop=True)
        k_df['diff'] = pd.to_timedelta(k_df['diff'])

        days = 365 / 2
        l_k_df = k_df.sort_values(['OwnerUserId', 'CreationDate'], ascending=[True, True]).groupby('OwnerUserId').nth(
            k).reset_index()
        c_k_df = l_k_df.sort_values(['OwnerUserId', 'CreationDate'], ascending=[True, True]).groupby(
            'OwnerUserId').filter(lambda x: x['diff'] >= pd.to_timedelta(str(days) + ' days'))
        nc_k_df = l_k_df.sort_values(['OwnerUserId', 'CreationDate'], ascending=[True, True]).groupby(
            'OwnerUserId').filter(lambda x: x['diff'] < pd.to_timedelta(str(days) + ' days'))

        Churn_users = c_k_df['OwnerUserId'].tolist()
        NonChurn_users = nc_k_df['OwnerUserId'].tolist()
        return (Churn_users,NonChurn_users, c_k_df, nc_k_df)

