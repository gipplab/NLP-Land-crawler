import math
import ast

import pandas as pd
import requests
import json
import time
from datetime import datetime
import numpy as np
import os
import random
from tqdm import tqdm


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


def map_name_to_id(df, col_name):
    df = df[["_id", col_name]].copy()
    df[col_name] = df[col_name].fillna('["unknown"]').apply(lambda x: ast.literal_eval(x)[0])
    return dict(zip(df[col_name], df["_id"]))


def update_papers(df_data, df_papers, df_venues, df_authors):
    rows = df_papers.shape[0]
    df_data = df_data.iloc[0:rows].copy()

    # update citations
    df_data["cites"] = df_data["inCitations"].fillna(0).apply(lambda x: x if (x == 0) else len(ast.literal_eval(x)))
    # dict_c = dict(zip(df_data["_id"], df_data["cites"]))

    # map venues
    df_venues = df_venues[["_id", "names"]].copy()
    df_venues["names"] = df_venues["names"].fillna('["unknown"]').apply(lambda x: ast.literal_eval(x)[0])
    dict_v = dict(zip(df_venues["names"], df_venues["_id"]))

    # map authors
    df_authors = df_authors[["_id", "fullname"]].copy()
    df_authors["fullname"] = df_authors["fullname"].fillna('"unknown"')
    dict_a = dict(zip(df_authors["fullname"], df_authors["_id"]))
    # print(dict_a)

    rows = df_papers.shape[0]
    for paper in tqdm(df_papers.iterrows(), total=rows):  # vectorized might be faster, but we have to sleep (wait for the server) at the end, so all improvement is lost anyway
        index = paper[0]
        id = paper[1]["_id"]
        data = {}

        # add random cites (correct amount)
        cites = df_data.loc[index, "cites"]
        if int(cites) > 0:
            index_list = random.sample(range(0, rows), cites)

            df_tmp = df_papers.iloc[index_list]
            data["cites"] = list(df_tmp["_id"])

        # add venue
        venues = dict_v[df_data.loc[index, "venue"]]
        data["venues"] = [venues]

        # add authors
        authors = []
        for author in ast.literal_eval(df_data.loc[index, "authors"]):
            authors.append(dict_a[author["name"]])
        data["authors"] = authors

        jdata = json.dumps(data)
        # print(jdata)

        res = requests.patch(f"http://localhost:3000/api/v0/papers/{id}", headers=my_headers, data=jdata)

        # print(res.json())
        if res.status_code != 200 or not res.ok:
            print(index, id, res)

        time.sleep(0.6)


if __name__ == '__main__':
    df_data = load_dataset()

    # post_papers(df_data)  # got stuck after 10751
    df_papers = get("df_papers.csv", "papers", 1000)
    # print(df_id)

    # post_venues(df_data)
    df_venues = get("df_venues.csv", "venues", 1000)

    # post_authors(df_data)
    df_authors = get("df_authors.csv", "authors", 1000)

    # update papers
    update_papers(df_data, df_papers, df_venues, df_authors)
