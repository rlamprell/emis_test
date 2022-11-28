import pandas as pd



def main():
    df = pd.read_json('emis_test\data\Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json')

    print(df)
    # print(df.to_string())
    print(df.columns.tolist())

    df = pd.DataFrame(df['entry'].values.tolist())
    print(df)
    print(df.columns.tolist())

    df = pd.DataFrame(df['resource'].values.tolist())
    print(df)
    print(df.columns.tolist())

if __name__ == '__main__':
    main()