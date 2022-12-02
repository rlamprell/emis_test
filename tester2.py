data = [
    {
        "url": "http: //hl7.org/fhir/us/core/StructureDefinition/us-core-race",
        "extension": [
            {
                "url": "ombCategory",
                "valueCoding": {
                    "system": "urn:oid: 2.16.840.1.113883.6.238",
                    "code": "2106-3",
                    "display": "White"
                }
            },
            {
                "url": "text",
                "valueString": "White"
            }
        ]
    },
    {
        "url": "http: //hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
        "extension": [
            {
                "url": "ombCategory",
                "valueCoding": {
                    "system": "urn:oid: 2.16.840.1.113883.6.238",
                    "code": "2186-5",
                    "display": "Not Hispanic or Latino"
                }
            },
            {
                "url": "text",
                "valueString": "Not Hispanic or Latino"
            }
        ]
    },
    {
        "url": "http: //hl7.org/fhir/StructureDefinition/patient-mothersMaidenName",
        "valueString": "Holley125 Champlin946"
    },
    {
        "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
        "valueCode": "M"
    },
    {
        "url": "http://hl7.org/fhir/StructureDefinition/patient-birthPlace",
        "valueAddress": {
            "city": "Barnstable",
            "state": "Massachusetts",
            "country": "US"
        }
    },
    {
        "url": "http://synthetichealth.github.io/synthea/disability-adjusted-life-years",
        "valueDecimal": 0.058264710382584003
    },
    {
        "url": "http://synthetichealth.github.io/synthea/quality-adjusted-life-years",
        "valueDecimal": 52.94173528961741
    }
]




import pandas as pd
import json

# json_data = json.loads(data)

# df = pd.DataFrame.from_dict(json_data)
# df = pd.DataFrame.from_dict(data)
# print(df)

# df = df.explode('extension')
# print(df)
# print(df.columns.tolist())

# print(f"\n\n\n\n\n\n\n")
# # print(pd.DataFrame(df['extension'].values.tolist(), index=df.index))

# print(df['extension'])
# print("hi")
# df_assign = pd.DataFrame()
# df_assign[["extension.url"]] = pd.DataFrame(df['extension'].values.tolist(), index=df.index)

# print(f"df_assign{df_assign}")
# df = df.join(df_assign).drop('extension', axis=1)
# print(df)

# df_resolv = pd.DataFrame()
# df_resolv[["resolvedBy.id", "resolvedBy.firstName", "resolvedBy.lastName"]] = pd.DataFrame(df['resolvedBy'].values.tolist(), index=df.index)
# df = df.join(df_resolv).drop('resolvedBy', axis=1)

# df_task = pd.json_normalize(data, record_path='task', meta=['id', 'state'])
df_task = pd.json_normalize(data)
# df = df.merge(df_task, on=['id', 'state', "assignee.id", "assignee.firstName", "assignee.lastName", "resolvedBy.id", "resolvedBy.firstName", "resolvedBy.lastName"], how="outer").drop('task', axis=1)
# print(df_task.to_string())
print(len(df_task))
for column in df_task:
    print(column)
    print(df_task[column])
    # print(any(isinstance(df_task[column], list)))
    # x = type(df_task[[column]]).eq(0).any(axis=1)
    # print(x)
    # [col for col, dt in df_task[column].dtypes.items() if dt == object]
    # []
    for value in df_task[column]:
        # print(f"\n value {value}")
        # print(value)
        print(type(value))
        if isinstance(value, list):
            print(value)
            df_normal = pd.json_normalize(df_task[column])
            print(df_normal)
            break


# print(pd.json_normalize(df_task['extension']))
# print(df_task['extension'])
print(df_task.columns.tolist())

# print(df.drop_duplicates().reset_index(drop=True))