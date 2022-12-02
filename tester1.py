data =[ {
'id': 123456,
'assignee':{'id':5757,'firstName':'Jim','lastName':'Johnson'},
'resolvedBy':{'id':5757,'firstName':'Jim','lastName':'Johnson'},
'task':[{
         'assignee':{'id':5757,'firstName':'Jim','lastName':'Johnson'},
         'resolvedBy':{'id':5757,'firstName':'Jim','lastName':'Johnson'},
         'taskId':898989,
         'status':'Closed'
        },
        {
         'assignee':{'id':5857,'firstName':'Nacy','lastName':'Johnson'},
         'resolvedBy':{'id':5857,'firstName':'George','lastName':'Johnson'},
         'taskId':999999
         }
       ],
'state':'Complete'
},
{
'id': 123477,
'assignee':{'id':8576,'firstName':'Jack','lastName':'Johnson'},
'resolvedBy':{'id':None,'firstName':None,'lastName':None},
'task':[],
'state':'Inprogress'
}]





import pandas as pd
import json

# json_data = json.loads(data)

# df = pd.DataFrame.from_dict(json_data)
df = pd.DataFrame.from_dict(data)

df = df.explode('task')
print(df)
print(df.columns.tolist())

print(f'\n\n\n\n\n\n\n')
print(pd.DataFrame(df['assignee'].values.tolist(), index=df.index))



df_assign = pd.DataFrame()
df_assign[['assignee.id', 'assignee.firstName', 'assignee.lastName']] = pd.DataFrame(df['assignee'].values.tolist(), index=df.index)

print(df_assign)
df = df.join(df_assign).drop('assignee', axis=1)
print(df)

df_resolv = pd.DataFrame()
df_resolv[['resolvedBy.id', 'resolvedBy.firstName', 'resolvedBy.lastName']] = pd.DataFrame(df['resolvedBy'].values.tolist(), index=df.index)
df = df.join(df_resolv).drop('resolvedBy', axis=1)

df_task = pd.json_normalize(data, record_path='task', meta=['id', 'state'])
df = df.merge(df_task, on=['id', 'state', 'assignee.id', 'assignee.firstName', 'assignee.lastName', 'resolvedBy.id', 'resolvedBy.firstName', 'resolvedBy.lastName'], how='outer').drop('task', axis=1)

print(df.drop_duplicates().reset_index(drop=True))