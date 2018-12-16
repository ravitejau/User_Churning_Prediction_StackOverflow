import os, sys
sys.path.insert(0, os.path.abspath(".."))
from features.get_temporal import get_temporal
from features.helper import helper
import seaborn as sn
import matplotlib.pyplot as plt
import pandas as pd

def plot_temporal(k):

    ##to get churn and nonchurn and make a dataframe
    labels=helper()
    x,y=labels.get_user_id(k)
    c_id=x
    nc_id=y
    print(len(c_id),len(nc_id))
    c_label=[1 for i in c_id]
    nc_label=[0 for i in nc_id]
    c_df=pd.DataFrame({'OwnerUserId': c_id,'label': c_label})
    nc_df=pd.DataFrame({'OwnerUserId': nc_id,'label': nc_label})
    fin_ldf=c_df.append(nc_df,ignore_index=True)
    print(fin_ldf.shape)

    ##Get our features
    t_df=get_temporal(k)
    print(t_df.shape)
    result_df = pd.merge(t_df, fin_ldf, on='OwnerUserId')
    print(result_df.shape)


    ##training and testing data split
    df_majority = result_df[result_df.label==0]
    df_minority = result_df[result_df.label==1]

    # df_maj_mean=df_majority.mean(axis=0)
    # df_min_mean=df_minority.mean(axis=0)
    nchurn=df_majority[1]
    churn=df_minority[1]
    print(nchurn)
    print(churn)
    
    plt.hist(list(churn),bins=20)
    plt.hist(list(nchurn),bins=20)
    plt.show()

plot_temporal(17)

    