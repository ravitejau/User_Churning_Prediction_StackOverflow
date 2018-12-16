import pandas as pd
import numpy as np
from datetime import datetime
import time
import helper as hp

fmt = '%Y-%m-%d %H:%M:%S'

class kl_feature:

    # users_df = pd.DataFrame()
    # complete_user_df = pd.DataFrame()
    # posts_df = pd.DataFrame()

    def get_all_knowledge_level_features(self, inp):

        k=inp
        x=hp.helper()

        user_df=pd.read_csv("../../data/processed/users.csv")
        user_df['Id']=user_df['Id'].astype(int)

        posts_df=pd.read_csv("../../data/processed/posts.csv")
        posts_df['Id']=posts_df['Id'].astype(int)

        c,nc=x.get_user_id(k)
        c_label = [1 for i in c]
        nc_label = [0 for i in nc]

        c_df = pd.DataFrame({'OwnerUserId': c, 'label': c_label})
        nc_df = pd.DataFrame({'OwnerUserId': nc, 'label': nc_label})
        feature_df = c_df.append(nc_df, ignore_index=True)


        li=list(range(k))

        k_df = posts_df.groupby('OwnerUserId').filter(lambda x: x['CreationDate'].count() >= k)
        k_df=k_df.sort_values(['OwnerUserId','CreationDate'], ascending=[True, True]).groupby('OwnerUserId').nth(li).reset_index()

        feature_df['acc_ans_rep']=feature_df.apply(lambda row: self.get_accepted_answerer_reputation(row,posts_df,user_df,k_df),axis=1)
        feature_df['max_rep_ansrr']=feature_df.apply(lambda row: self.get_max_reputation_answerer(row,posts_df,user_df,k_df),axis=1)
        feature_df['num_que_answered'] = feature_df.apply(lambda row: self.get_num_questions_answered(row, posts_df, user_df, k_df),axis=1)
        feature_df['rep_questioner'] = feature_df.apply(lambda row: self.get_rep_questioner(row, posts_df, user_df, k_df),axis=1)
        feature_df['num_answers_recvd'] = feature_df.apply(lambda row: self.get_num_answers_recvd(row, posts_df, user_df, k_df),axis=1)

        feature_df['rep_answerers'] = feature_df.apply(lambda row: self.get_rep_answerers(row, posts_df, user_df, k_df),axis=1)
        feature_df['rep_co_answerers'] = feature_df.apply(lambda row: self.get_rep_co_answerers(row, posts_df, user_df, k_df),axis=1)

        print(feature_df.shape)
        #print(feature_df.tail(20))

        return feature_df


    def get_accepted_answerer_reputation(self,row,posts_df,user_df,k_df):
        temp_k_df=k_df[k_df.OwnerUserId==row.OwnerUserId]
        temp_k_df=temp_k_df[temp_k_df.PostTypeId==1]
        acc_ans_id=temp_k_df['AcceptedAnswerId'][temp_k_df.AcceptedAnswerId.notna()].tolist()
        acc_ans_id=[int(i) for i in acc_ans_id]
        if len(acc_ans_id)==0:
            return 0

        mask = posts_df['Id'].isin(acc_ans_id)
        temp_post_df=posts_df[mask]
        acc_ans_ownerid=temp_post_df['OwnerUserId'][temp_post_df.OwnerUserId.notna()].tolist()
        acc_ans_ownerid=list(set(acc_ans_ownerid))
        acc_ans_ownerid=[int(i) for i in acc_ans_ownerid]


        mask = user_df['Id'].isin(acc_ans_ownerid)
        temp_user_df=user_df[mask]
        Rep_list=temp_user_df['Reputation'][temp_user_df.Reputation.notna()].tolist()
        try:
            return sum(Rep_list)/len(Rep_list)
        except:
            return 0


    def get_max_reputation_answerer(self,row,posts_df,user_df,k_df):
        temp_k_df=k_df[k_df.OwnerUserId==row.OwnerUserId]
        temp_k_df=temp_k_df[temp_k_df.PostTypeId==1]
        ques_id=temp_k_df['Id'][temp_k_df.Id.notna()].tolist()
        

        temp_ans_df=posts_df[posts_df['ParentId'].isin(ques_id)]
        topi=temp_ans_df.groupby('ParentId').ngroups
        ownerid=temp_ans_df['OwnerUserId'][temp_ans_df.OwnerUserId.notna()].tolist()
        mask = user_df['Id'].isin(ownerid)
        temp_user_df=user_df[mask]
        Rep_list=temp_user_df['Reputation'][temp_user_df.Reputation.notna()].tolist()
        tx=sorted(Rep_list)[-topi:]
        try:
            return sum(tx)/len(tx)
        except:
            return 0


    def get_num_questions_answered(self,row,posts_df,user_df,k_df):
        post_id_list = k_df[(k_df.OwnerUserId == row.OwnerUserId) & (k_df.PostTypeId == 1)]['Id'].tolist()
        post_id_list = [int(i) for i in post_id_list]
        if len(post_id_list) == 0:
            return 0
        mask = posts_df['ParentId'].isin(post_id_list)
        answer_df = posts_df[mask].groupby('ParentId').count()
        try:
            return answer_df.shape[0]/len(post_id_list)
        except:
            return 0

    def get_time_for_first_answer(self, users_df, complete_user_df, posts_df):
        user_to_mean_time_for_first_answ = []
        for index, user in users_df.iterrows():
            # user_question_post_id_df
            df = posts_df[(posts_df.OwnerUserId == user.OwnerUserId) &
                          (posts_df.PostTypeId == 1)][['Id', 'CreationDate']]
            first_answered_time_list = []

            for index, row in df.iterrows():
                question_date = row['CreationDate']
                answer_df = posts_df[posts_df.ParentId == row['Id']].sort_values(['CreationDate'], ascending=[True])
                first_answered_date = answer_df.iloc[0].CreationDate
                diff = first_answered_date - question_date
                # print("Difference Date : " + str(diff))
                first_answered_time_list.append(diff/np.timedelta64(1, 'm'))

            if len(first_answered_time_list) > 0:
                mean_response_time = sum(first_answered_time_list) / len(first_answered_time_list)
                user_to_mean_time_for_first_answ.append({'userid': user.OwnerUserId, 'time_for_first_answer': mean_response_time})

        time_for_first_answer_df = pd.DataFrame(user_to_mean_time_for_first_answ)
        return time_for_first_answer_df

    def get_rep_questioner(self,row,posts_df,user_df,k_df):
        temp_k_df=k_df[k_df.OwnerUserId==row.OwnerUserId]
        temp_k_df=temp_k_df[temp_k_df.PostTypeId==2]
        parent_id=temp_k_df['ParentId'][temp_k_df.ParentId.notna()].tolist()
        
        temp_ans_df=posts_df[posts_df['Id'].isin(parent_id)]
        topi=temp_ans_df.groupby('ParentId').ngroups
        ownerid=temp_ans_df['OwnerUserId'][temp_ans_df.OwnerUserId.notna()].tolist()
        mask = user_df['Id'].isin(ownerid)
        temp_user_df=user_df[mask]
        Rep_list=temp_user_df['Reputation'][temp_user_df.Reputation.notna()].tolist()
        
        try:
            return sum(Rep_list)/len(Rep_list)
        except:
            return 0


    def get_rep_answerers(self, row, posts_df, user_df, k_df):
        post_id_df = k_df[(k_df.OwnerUserId == row.OwnerUserId) & (k_df.PostTypeId == 1)]
        post_id_list = post_id_df['Id'].tolist()
        post_id_list = [int(i) for i in post_id_list]
        if len(post_id_list) == 0:
            return 0

        mask = posts_df['ParentId'].isin(post_id_list)
        uid_list = posts_df[mask]['OwnerUserId'][posts_df.OwnerUserId.notna()].tolist()
        if len(uid_list) == 0:
            return 0
        uid_list = [int(i) for i in uid_list]
        uid_set = set(uid_list)
        return user_df[user_df.Id.isin(uid_set)].Reputation.mean()


    def get_rep_co_answerers(self, row, posts_df, user_df, k_df):
        post_id_df = k_df[(k_df.OwnerUserId == row.OwnerUserId) & (k_df.PostTypeId == 2)]
        parent_id_list = post_id_df['ParentId'].tolist()
        parent_id_list = [int(i) for i in parent_id_list]
        if len(parent_id_list) == 0:
            return 0

        mask = posts_df['ParentId'].isin(parent_id_list)
        filtered_posts_df = posts_df[mask][posts_df.OwnerUserId != row.OwnerUserId]
        uid_list = filtered_posts_df['OwnerUserId'][filtered_posts_df.OwnerUserId.notna()].tolist()
        if len(uid_list) == 0:
            return 0
        uid_list = [int(i) for i in uid_list]
        uid_set = set(uid_list)
        return user_df[user_df.Id.isin(uid_set)].Reputation.mean()

    def get_num_answers_recvd(self,row,posts_df,user_df,k_df):
        temp_k_df=k_df[k_df.OwnerUserId==row.OwnerUserId]
        temp_k_df=temp_k_df[temp_k_df.PostTypeId==1]
        que_id=temp_k_df['Id'][temp_k_df.Id.notna()].tolist()
        temp_ans_df=posts_df[posts_df['ParentId'].isin(que_id)]
        ans=temp_ans_df.shape[0]
        
        try:
            return ans/len(que_id)
        except:
            return 0

