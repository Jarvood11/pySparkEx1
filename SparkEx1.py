# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 01:35:12 2023

@author: ewent
"""

import pandas as pd
from typing import Dict
from pyspark.sql import SparkSession
from fugue import transform

input_df = pd.DataFrame({"id":[0,1,2], "value": (["A", "B", "C"])})
map_dict = {"A": "Apple", "B": "Banana", "C": "Carrot"}

def map_letter_to_food(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df["value"] = df["value"].map(mapping)
    return df

spark = SparkSession.builder.getOrCreate()

# use Fugue transform to switch execution to spark
df = transform(input_df,
               map_letter_to_food,
               schema="*",
               params=dict(mapping=map_dict),
               engine=spark
               )

print(type(df))
df.show()