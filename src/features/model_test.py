import helper as hp
import get_temporal as gt
import pandas as pd
import numpy as np
from sklearn.utils import resample
from sklearn import svm
import sklearn.metrics as sm
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn import tree

k=5

##to get churn and nonchurn and make a dataframe
labels=hp.helper()
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
t_df=gt.get_temporal(k)
print(t_df.shape)
result_df = pd.merge(t_df, fin_ldf, on='OwnerUserId')
print(result_df.shape)


##training and testing data split

df_majority = result_df[result_df.label==0]
df_minority = result_df[result_df.label==1]

df_minority_upsampled = resample(df_minority,replace=True,n_samples=df_majority.shape[0],random_state=123)

result_df = pd.concat([df_majority, df_minority_upsampled])


msk = np.random.rand(len(result_df)) < 0.8
train_df = result_df[msk]
test_df = result_df[~msk]
df_upsampled=train_df.copy()


y = df_upsampled.label
X = df_upsampled.drop('label', axis=1)

y_test=test_df.label
x_test=test_df.drop('label', axis=1)

#model = svm.SVC(kernel='linear', class_weight='balanced', C=1, gamma=1)
model= tree.DecisionTreeClassifier()
scaler = StandardScaler()

X_std = scaler.fit_transform(X)
x_test_std = scaler.fit_transform(x_test)

model.fit(X_std, y)
predicted= model.predict(x_test_std)

print('training score',model.score(X_std, y))
print('testing score',sm.accuracy_score(y_test,predicted))
print('f1 score',sm.f1_score(y_test,predicted))
print('recall',sm.recall_score(y_test,predicted))
print('precision',sm.precision_score(y_test,predicted))

