import xml.etree.cElementTree as et
import pandas as pd
 
def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None
 


def users():
    """ Convert PostHistory.xml to pandas dataframe """

    parsed_xml = et.parse("../../data/raw/Users.xml")
    dfcols = ['Id','AccountId','CreationDate', 'Reputation','Views','UpVotes','DownVotes','LastAccessDate']
    df_xml = pd.DataFrame(columns=dfcols)
    i=0
    for node in parsed_xml.getroot():
        if i%10000==0:
            print(i,df_xml.shape)
        i+=1
        Id=node.attrib.get('Id')
        AccountId = node.attrib.get('AccountId')
        CreationDate = node.attrib.get('CreationDate')
        Reputation = node.attrib.get('Reputation')
        Views = node.attrib.get('Views')
        UpVotes = node.attrib.get('UpVotes')
        DownVotes = node.attrib.get('DownVotes')
        LastAccessDate = node.attrib.get('LastAccessDate')
 
        df_xml = df_xml.append(
            pd.Series([Id,AccountId, CreationDate, Reputation,
                       Views,UpVotes,DownVotes,LastAccessDate], index=dfcols),
            ignore_index=True)
 
    return df_xml
 
users_df_xml=users()
users_df_xml.to_csv("../../data/processed/users.csv",index=False)