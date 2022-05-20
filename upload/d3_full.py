import csv
import math
import ast
import string
import sys
import gc
import pandas as pd
import requests
import json
import time
from datetime import datetime
import numpy as np
import os
import random
from tqdm import tqdm

csv.field_size_limit(sys.maxsize)  # part 4 has a field that is too large
token = open(".env").read()
my_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# use "head -n 15003 d3_2021_12.csv > d3_11k.csv" in terminal to create csv
def load_dataset(path: string) -> pd.DataFrame:
    return pd.read_csv(
        path, header=0, index_col=0, engine="python"
    )


def post(df, route, size=50):
    rows = df.shape[0]
    for i in range(math.ceil(rows/size)):
        if i*size < rows:
            df_part = df.iloc[i * size:(i + 1) * size]
        else:
            df_part = df.iloc[i*size: rows]
        # df_part = df.iloc[9: 10]
        jdata = df_part.to_json(orient="records")
        res = requests.post(f"http://localhost:3000/api/v0/{route}", headers=my_headers, data=jdata)
        print(res)
        # print(res.json())
        # exit()


def check_for_data(route):
    res = requests.get(f"http://localhost:3000/api/v0/{route}?limit=1", headers=my_headers)
    return len(res.json()) > 0


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
        df.to_csv(filename)
    else:
        df = pd.read_csv(filename)
    return df



def transform_papers(df):
    # populated columns drop
    df = df.drop(columns=["@mdate", "pages", "authors", "volume", "number", "sources",
                          "journalVolume", "venue", "journalName", "journalPages", "doiUrl"])

    # sparse columns drop
    df = df.drop(columns=["pmid", "entities", "crossref", "cdrom", "cite", "note", "@publtype",
                          "booktitle", "isbn", "series", "address", "editor", "chapter", "@cdate",
                          "address"])

    df = df.rename(columns={"id": "csvId", "paperAbstract": "abstractText", "journal": "venueRaw",
                            "@key": "dblpId", "year": "yearPublished", "type": "typeOfPaper",
                            "inCitations": "inCitationsRef", "outCitations": "outCitationsRef", "author": "authorsRaw"})
    df["absUrl"] = "https://dblp.org/" + df["url"]
    df = df.drop(columns=["url"])

    # TODO fieldsOfStudy, publisher extra Table/collection?
    # what columns do we keep in the database?

    df["inCitationsCount"] = 0
    df["outCitationsCount"] = 0
    df["inCitationsRef"] = df["inCitationsRef"].fillna({i: [] for i in df.index})  # empty List
    df["outCitationsRef"] = df["outCitationsRef"].fillna({i: [] for i in df.index})  # empty List
    # df["abstractText"] = df["abstractText"].fillna("")
    df["pdfUrls"] = df["pdfUrls"].apply(lambda x: ast.literal_eval(x))
    df["fieldsOfStudy"] = df["fieldsOfStudy"].apply(lambda x: ast.literal_eval(x))

    # columns maybe keep
    df = df.drop(columns=["month", "school"])

    post(df, "papers", 25)


def post_venues(df_data, i):
    filename = f"d3_venues_{i}.csv"
    df_venues = pd.DataFrame({})
    if not os.path.isfile(filename):
        df_tmp = pd.DataFrame({})

        df_tmp["names"] = df_data["journal"].unique()

        if i != 1:
            df_venues = pd.read_csv(f"d3_venues_{i-1}.csv", header=0, index_col=0)
            df_venues = pd.concat([df_venues, df_tmp])
            df_venues = df_venues.drop_duplicates()
        else:
            df_venues = df_tmp

        df_venues.to_csv(filename)

        del df_tmp

    if i == 4 and not check_for_data("venues"):
        if df_venues.empty:
            df_venues = pd.read_csv(f"d3_venues_4.csv", header=0, index_col=0)
        post(df_venues, "venues", 1000)
    del df_venues
    gc.collect()


# def post_authors(df):
#     df = df["authors"].apply(lambda x: ast.literal_eval(x))  # convert string to object
#     df = df.explode(ignore_index=True)  # list to single elements
#     df = df.apply(pd.Series)
#     df = df.drop(columns=["structuredName", 0, "ids"])
#     df = df.drop_duplicates()
#     df = df.rename(columns={"name": "fullname"})
#     df = df.fillna("unknown")
#     df["email"] = "test@test.com"
#     df["dblpId"] = "string"
#     df["createdBy"] = "622f525105bda66eba41510d"
#     df["createdAt"] = datetime.now()
#     post(df, "authors", 2)


def map_name_to_id(df, col_name):
    df = df[["_id", col_name]].copy()
    df[col_name] = df[col_name].fillna('["unknown"]').apply(lambda x: ast.literal_eval(x)[0])
    return dict(zip(df[col_name], df["_id"]))


# def update_papers(df_data, df_papers, df_venues, df_authors):
#     rows = df_papers.shape[0]
#     df_data = df_data.iloc[0:rows].copy()
#     df_data["venue"] = df_data["venue"].fillna("unknown")
#
#     # update citations
#     df_data["cites"] = df_data["inCitations"].fillna(0).apply(lambda x: x if (x == 0) else len(ast.literal_eval(x)))
#
#     # map venues
#     df_venues = df_venues[["_id", "names"]].copy()
#     df_venues["names"] = df_venues["names"].fillna('["unknown"]').apply(lambda x: ast.literal_eval(x)[0])
#     dict_v = dict(zip(df_venues["names"], df_venues["_id"]))
#
#     # map authors
#     df_authors = df_authors[["_id", "fullname"]].copy()
#     df_authors["fullname"] = df_authors["fullname"].fillna('"unknown"')
#     dict_a = dict(zip(df_authors["fullname"], df_authors["_id"]))
#
#     rows = df_papers.shape[0]
#     for paper in tqdm(df_papers.iterrows(), total=rows):  # vectorized would be faster
#         index = paper[0]
#         id = paper[1]["_id"]
#         data = {}
#
#         # add random cites (wrong citations)
#         cites = df_data.loc[index, "cites"]
#         if int(cites) > 0:
#             index_list = random.sample(range(0, rows), cites)
#
#             df_tmp = df_papers.iloc[index_list]
#             data["cites"] = list(df_tmp["_id"])
#
#         # add venue
#         venues = dict_v[df_data.loc[index, "venue"]]
#         data["venues"] = [venues]
#
#         # add authors
#         authors = []
#         for author in ast.literal_eval(df_data.loc[index, "authors"]):
#             authors.append(dict_a[author["name"]])
#         data["authors"] = authors
#
#         jdata = json.dumps(data)
#         # print(jdata)
#
#         res = requests.patch(f"http://localhost:3000/api/v0/papers/{id}", headers=my_headers, data=jdata)
#
#         # print(res.json())
#         if res.status_code != 200 or not res.ok:
#             print(index, id, res)


# split file using: https://stackoverflow.com/questions/20721120/how-to-split-csv-files-as-per-number-of-rows-specified
# splitCsv() {
#     HEADER=$(head -1 $1)
#     if [ -n "$2" ]; then
#         CHUNK=$2
#     else
#         CHUNK=1000
#     fi
#     tail -n +2 $1 | split -l $CHUNK - $1_split_
#     for i in $1_split_*; do
#         sed -i -e "1i$HEADER" "$i"
#     done
# }

def analyse_cols(df_data: pd.DataFrame):
    empty_cols = []
    sparse_columns = ["pmid", "entities", "crossref", "cdrom", "cite", "note", "@publtype", "month",
                      "booktitle", "publisher", "school", "isbn", "series", "editor", "chapter",
                      "@cdate", "address"]

    # sparse_columns = ["type"]  # ['article' 'inproceedings' 'phdthesis' 'book' 'incollection' 'proceedings']
    # sparse_columns = ["sources"]  # ["['DBLP']" "['Medline', 'DBLP']" "['DBLP', 'Medline']"]

    for column in df_data.columns:
        if df_data[column].isnull().all():
            empty_cols.append(column)
        elif column in sparse_columns:
            print(f"Unique values in {column}:\n{df_data[column].unique()}")
    print(f"Empty columns: {empty_cols}")


def compare_cols(df_data: pd.DataFrame):
    df_data2 = df_data[["journal", "venue"]]
    subsetDataFrame2 = df_data2[df_data2['journal'] != df_data2["venue"]]
    print(subsetDataFrame2)

    df_data = df_data[["journal", "journalName"]]
    subsetDataFrame = df_data[df_data['journal'] != df_data["journalName"]]
    print(subsetDataFrame)


def main(df_data: pd.DataFrame, i:int) -> None:
    # analyse_cols(df_data)
    # compare_cols(df_data)
    # print(df_data[df_data["paperAbstract"].isnull()]["paperAbstract"])
    # transform_papers(df_data)
    post_venues(df_data, i)
    pass
# # post_papers(df_data)  # got stuck after 10751
# df_papers = get("df_papers.csv", "papers", 1000)
# # print(df_id)
#
# # post_venues(df_data)
# df_venues = get("df_venues.csv", "venues", 1000)
#
# # post_authors(df_data)
# df_authors = get("df_authors.csv", "authors", 1000)
#
# update_papers(df_data, df_papers, df_venues, df_authors)


if __name__ == '__main__':
    mode = "test"
    # mode = "real"
    if mode == "test":
        print("test mode")
        df_data = load_dataset("d3_11k.csv")
        main(df_data, 4)
        # main(df_data, 4)
    else:
        print("full data mode")
        start = time.time()
        for i in range(1, 5):
            if i == 4:
                gc.collect()
                print(f"processing part {i}/4...")
                start_part = time.time()
                df_data = load_dataset(f"d3_{i}.csv")
                main(df_data, i)
                print(f"Part {i}/4 took {(time.time()-start_part) / 60} min")
                del df_data
                gc.collect()
        print(f"Total time: {(time.time() - start)/60} min")