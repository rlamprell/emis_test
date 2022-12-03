from flatten_json import flatten_json
import pandas as pd

data = {
    "id": 1,
    "class": "c1",
    "owner": "myself",
    "metadata": {
        "m1": {
            "value": "m1_1",
            "timestamp": "d1"
        },
        "m2": {
            "value": "m1_2",
            "timestamp": "d2"
        },
        "m3": {
            "value": "m1_3",
            "timestamp": "d3"
        },
        "m4": {
            "value": "m1_4",
            "timestamp": "d4"
        }
    },
    "a1": {
        "a11": [

        ]
    },
    "m1": {},
    "comm1": "COMM1",
    "comm2": "COMM21529089656387",
    "share": "xxx",
    "share1": "yyy",
    "hub1": "h1",
    "hub2": "h2",
    "context": [

    ]
}


df = pd.DataFrame([flatten_json(data)])
print(df)