import math
import ast

import pandas as pd
import requests
import json
import time
from datetime import datetime
import numpy as np
import os


token = open(".env").read()
my_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# use "head -n 15003 d3_2021_12.csv > d3_11k.csv" in terminal to create csv
def load_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "d3_11k.csv", low_memory=False, header=0, index_col=0
    )


def post(df, route, size=50):
    rows = df.shape[0]
    for i in range(math.ceil(rows/size)):
        print(i)
        if i*size < rows:
            df_part = df.iloc[i * size:(i + 1) * size]
        else:
            df_part = df.iloc[i*size: rows]
        jdata = df_part.to_json(orient="records")
        res = requests.post(f"http://localhost:3000/api/v0/{route}", headers=my_headers, data=jdata)
        print(res)
        # print(res.json())
        time.sleep(0.6)


def get(filename, route, size=1000):
    if not os.path.isfile(filename):
        df = pd.DataFrame({})
        done = False
        skip = 0
        while not done:
            res = requests.get(f"http://localhost:3000/api/v0/{route}?skip={skip}&limit={size}", headers=my_headers)
            df_part = pd.DataFrame.from_dict(res.json())
            skip += size
            if df_part.empty:
                done = True
            else:
                df = pd.concat([df, df_part])
            time.sleep(0.6)
        df.to_csv(filename)
    else:
        df = pd.read_csv(filename)
    return df


def transform_papers(df):
    df = df.rename(columns={"id": "dblpId", "paperAbstract": "abstractText" })
    df["citationInfoTimestamp"] = datetime.now()
    df["absUrl"] = "https://dblp.org/" + df["url"]
    df["pdfUrl"] = "url/url"
    df["isStudentPaper"] = True
    df["isSharedTask"] = True
    df["atMainConference"] = True
    df["shortOrLong"] = "long"
    df["preProcessingGitHash"] = "hash-test"
    df["abstractExtractor"] = "grobid"
    df["typeOfPaper"] = "journal"
    df["datePublished"] = "2/2/" + df["year"].astype(int).astype(str)  # incorrect time zone
    df["createdBy"] = "622f525105bda66eba41510d"
    df["createdAt"] = datetime.now()

    df = df.drop(columns=["ee", "isbn", "address", "doiUrl", "outCitations", "@key", "author",
                                "pages", "year", "month", "@mdate", "volume", "journal", "number", "type",
                                "sources", "journalName", "@cdate", "editor", "authors", "school", "publisher",
                                "chapter", "inCitations", "cdrom", "note", "entities", "crossref", "journalPages",
                                "pmid", "journalVolume", "fieldsOfStudy", "@publtype", "pdfUrls", "venue", "cite",
                                "booktitle", "series", "url"])
    post(df, "papers", 50)


def post_venues(df_data):
    df = pd.DataFrame({})
    df["names"] = df_data["venue"].unique()
    df["dblpId"] = "string"
    df["createdBy"] = "622f525105bda66eba41510d"
    df["createdAt"] = datetime.now()
    print()

    post(df, "venues", 1000)


def post_authors(df):
    df = df["authors"].apply(lambda x: ast.literal_eval(x))  # convert string to object
    df = df.explode(ignore_index=True)  # list to single elements
    df = df.apply(pd.Series)
    df = df.drop(columns=["structuredName", 0, "ids"])
    df = df.drop_duplicates()
    df = df.rename(columns={"name": "fullname"})
    df = df.fillna("unknown")
    df["email"] = "test@test.com"
    df["dblpId"] = "string"
    df["createdBy"] = "622f525105bda66eba41510d"
    df["createdAt"] = datetime.now()
    post(df, "authors", 2)


if __name__ == '__main__':
    df_data = load_dataset()

    # post papers
    # transform_papers(dataset)  # got stuck after 10750 or 10751
    # get papers (for references)
    # df_papers = get("df_papers.csv", "papers", 1000)
    # print(df_id)
    # post venues
    # post_venues(df_data)
    # df_venues = get("df_venues.csv", "venues", 1000)
    # post authors
    # post_authors(df_data)
    # get authors
    # df_authors = get("df_authors.csv", "authors", 1000)

    # update papers
