import pandas as pd



def main():
    df = pd.read_json('data\Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json')

    print(df)
    # print(df.to_string())
    print(df.columns.tolist())

    df2 = pd.DataFrame(df['entry'].values.tolist())
    print(df2)
    print(df2.columns.tolist())

    df3 = pd.DataFrame(df2['resource'].values.tolist())
    print(df3)
    print(df3.columns.tolist())
    
    print("\n\n")
    # FIELDS = ["key", "fields.summary", "fields.issuetype.name", "fields.status.name", "fields.status.statusCategory.name"]

    # max_level chosen arbitrarily
    df4 = pd.json_normalize(df["entry"])
    # df4[FIELDS]
    print(df4)
    print(df4.columns.tolist())
    # print(df4.to_string())

    x = df4.loc[df4['fullUrl'] == 'urn:uuid:fbe05d5f-04fb-ebe0-608d-cd9f39d1253e']

    print(x["resource.agent"])


if __name__ == '__main__':
    main()