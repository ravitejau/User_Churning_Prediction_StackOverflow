import xml.etree.cElementTree as et
import pandas as pd
 
def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None
 


def users():
    """ Convert PostHistory.xml to pandas dataframe """

    parsed_xml = et.parse("../../data/raw/Posts.xml")
    dfcols = ['Id','OwnerUserId','PostTypeId', 'AcceptedAnswerId', 'ParentId', 'CreationDate', 'Score','LastEditorUserId','LastEditDate','LastActivityDate','CommentCount']
    df_xml = pd.DataFrame(columns=dfcols)
    i=0
    for node in parsed_xml.getroot():
        if i%10000==0:
            print(i,df_xml.shape)
        i+=1
        Id=node.attrib.get('Id')
        OwnerUserId = node.attrib.get('OwnerUserId')
        PostTypeId = node.attrib.get('PostTypeId')
        AcceptedAnswerId = node.attrib.get('AcceptedAnswerId')
        ParentId = node.attrib.get('ParentId')
        CreationDate = node.attrib.get('CreationDate')
        Score = node.attrib.get('Score')
        LastEditorUserId = node.attrib.get('LastEditorUserId')
        LastEditDate = node.attrib.get('LastEditDate')
        LastActivityDate = node.attrib.get('LastActivityDate')
        CommentCount = node.attrib.get('CommentCount')
 
        df_xml = df_xml.append(
            pd.Series([Id,OwnerUserId,PostTypeId, AcceptedAnswerId, ParentId,
                       CreationDate, Score,LastEditorUserId,LastEditDate,LastActivityDate,CommentCount], index=dfcols),
            ignore_index=True)
 
    return df_xml
 
posts_df_xml=users()
posts_df_xml.to_csv("../../data/processed/posts.csv",index=False)