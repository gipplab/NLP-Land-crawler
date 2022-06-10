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
import os
from tqdm import tqdm
import logging as log

csv.field_size_limit(sys.maxsize)  # part 4 has a field that is too large
token = open(".env").read()
my_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
log.basicConfig(format='%(message)s', stream=sys.stdout, level=log.INFO)


def read_csv(filename):
    return pd.read_csv(filename, header=0, index_col=0, dtype={"number": str, "absUrl": str})  # TODO define dtype for all columns


def load_dataset(path: string) -> pd.DataFrame:
    return pd.read_csv(
        path, header=0, index_col=0, engine="python"
    )


def post(df_data, route, size=50):
    rows = df_data.shape[0]
    for i in tqdm(range(math.ceil(rows/size)), total=math.ceil(rows/size)):
        if i*size < rows:
            df_part = df_data.iloc[i * size:(i + 1) * size].copy()
        else:
            df_part = df_data.iloc[i * size: rows].copy()
        jdata = df_part.to_json(orient="records")
        res = requests.post(f"http://localhost:3000/api/v0/{route}", headers=my_headers, data=jdata)
        if res.status_code >= 400:
            log.error(res)
            log.error(res.json())
            # log.error(df_part)


def check_for_data(route):
    res = requests.get(f"http://localhost:3000/api/v0/{route}?limit=1", headers=my_headers)
    return len(res.json()) > 0


def list_venues(df_data, filename):
    log.info("Create venues list")
    if not os.path.isfile(filename):
        start = time.time()
        df_venues = pd.DataFrame({})
        df_venues["names"] = df_data["journal"].dropna().unique()
        df_venues.to_csv(filename)
        del df_venues
        log.info(f"This took {(time.time() - start) / 60} min")
    gc.collect()


def eval_author(x):
    if pd.isna(x):  # nan
        return []
    elif x.startswith("["):  # list of authors, correctly formatted
        return json.loads(x)
    elif x.startswith("{"):  # one author with orcid and incorrectly formatted json
        x = x.replace("\"", "\\\"")  # nicknames with " "
        x = x.replace("{@orcid: ", "{\"@orcid\": \"")
        x = x.replace("{@aux: ", "{\"@aux\": \"")   # very rare case
        x = x.replace(", @orcid: ", "\", \"@orcid\": \"")
        x = x.replace(", #text: ", "\", \"#text\": \"")
        x = x.replace("}", "\"}")
        return [json.loads(x)]
    else:  # normal names
        return [x]


def list_authors(df_data, filename):
    def split_author(author):  # the most expensive part for authors
        orcid = None
        number = None
        if isinstance(author, dict):
            orcid = author["@orcid"]
            fullname = author["#text"]
        else:
            fullname = author
        split = fullname.split(" ")
        if split[-1].isnumeric():
            number = split[-1]
            fullname = " ".join(split[0: -1])
        return pd.Series([orcid, fullname, number])

    log.info("Create authors list")
    if not os.path.isfile(filename):
        start = time.time()
        df_authors = pd.DataFrame({})
        df_authors["author"] = df_data["author"].apply(eval_author)
        df_authors = df_authors.explode("author", ignore_index=True)  # list to single elements
        before = df_authors.shape[0]
        df_authors = df_authors.dropna()
        log.info(f"Removed {before - df_authors.shape[0]} entries that were nan")
        df_authors = df_authors["author"].apply(split_author)
        log.info("Past split row")
        df_authors = df_authors.set_axis(["orcid", "fullname", "number"], axis=1)
        df_authors.to_csv(filename)
        log.info(f"This took {(time.time() - start) / 60} min")
        del df_authors
    gc.collect()


def combine_table(mode, table):
    log.info(f"Combine {table} into single CSV")
    filename = f"d3_{table}_{mode}.csv"
    if not os.path.isfile(filename):
        start = time.time()
        df_table = pd.DataFrame({})
        for i in range(1, 5):
            df_new = read_csv(f"d3_{table}_{mode}_{i}.csv")
            df_table = pd.concat([df_table, df_new])
        before = df_table.shape[0]
        if table == "authors":
            df_table = df_table.sort_values(by=["orcid"], na_position="last")
            df_table = df_table.drop_duplicates(subset=["fullname", "number"], keep="first")
        else:
            df_table = df_table.drop_duplicates()
        log.info(f"Removed {before - df_table.shape[0]} duplicates")
        df_table.to_csv(filename)
        log.info(f"This took {(time.time() - start) / 60} min")


def post_table(table, mode, size=1000):
    log.info(f"Post {table} to the backend")
    if not check_for_data(table):
        start = time.time()
        log.info(f"Loading {table} from file")
        df_table = read_csv(f"d3_{table}_{mode}.csv")
        log.info(f"Sending {table} to backend")
        post(df_table, table, size)
        log.info(f"This took {(time.time() - start) / 60} min")


def post_papers(mode):
    # if not check_for_data("papers"):
    for i in range(1, 5):
        # if i == 4:
        start = time.time()
        log.info(f"Post papers {i}/4 to the backend")
        log.info(f"Loading papers {i}/4 from file")
        df_papers = read_csv(f"d3_{mode}_edit_{i}.csv")

        log.info(f"Converting fields with lists from string to list again")
        df_papers["pdfUrls"] = df_papers["pdfUrls"].apply(lambda x: x if pd.isnull(x) else ast.literal_eval(x))
        df_papers["authors"] = df_papers["authors"].apply(ast.literal_eval)
        df_papers["fieldsOfStudy"] = df_papers["fieldsOfStudy"].apply(lambda x: [] if pd.isnull(x) else ast.literal_eval(x))

        log.info(f"Sending papers {i}/4 to backend")
        post(df_papers, "papers", 10000)
        log.info(f"This took {(time.time() - start) / 60} min")
        del df_papers
        gc.collect()


def get_table(route, mode, size=1000):
    filename = f"d3_{route}_{mode}_ids.csv"
    if not os.path.isfile(filename):
        log.info(f"Get {route} from the backend")
        start = time.time()
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
        df = df.drop(columns=["createdAt", "createdBy", "__v"])
        df.to_csv(filename)
        log.info(f"This took {(time.time() - start) / 60} min")
    else:
        log.info(f"Load {route} from file")
        df = read_csv(filename)
        if route == "venues":
            df["names"] = df["names"].apply(lambda x: ast.literal_eval(x))
    return df


def map_venue_name_to_id(df):
    df["names"] = df["names"].apply(lambda x: x[0])
    return dict(zip(df["names"], df["_id"]))


def map_author_name_to_id(df):
    df["name"] = df.apply(lambda row: row["fullname"] if pd.isnull(row["number"]) else f"{row['fullname']} {row['number']}", axis=1)
    return dict(zip(df["name"], df["_id"]))


def transform_papers(df, filename, dict_venues, dict_authors):
    def map_author(author):
        authors_raw = eval_author(author)
        authors = []
        for author in authors_raw:
            if isinstance(author, dict):
                authors.append(dict_authors.get(author["#text"]))
            else:
                authors.append(dict_authors.get(author))
        return authors

    log.info("Drop and rename columns")
    # populated columns drop
    df = df.drop(columns=["@mdate", "pages", "authors", "volume", "number", "sources",
                          "journalVolume", "venue", "journalName", "journalPages", "doiUrl"])

    # sparse columns drop
    df = df.drop(columns=["pmid", "entities", "crossref", "cdrom", "cite", "note", "@publtype",
                          "booktitle", "isbn", "series", "address", "editor", "chapter", "@cdate",
                          "address"])

    # columns maybe keep
    df = df.drop(columns=["month", "school"])

    df = df.rename(columns={"id": "datasetId", "paperAbstract": "abstractText", "journal": "venueRaw",
                            "@key": "dblpId", "year": "yearPublished", "type": "typeOfPaper",
                            "inCitations": "inCitationsRef", "outCitations": "outCitationsRef",
                            "author": "authorsRaw"})
    log.info("Set urls")
    # df["absUrl"] = "https://dblp.org/" + df["url"]
    # df = df.drop(columns=["url"])

    log.info("Set citations")
    df["inCitationsCount"] = df["inCitationsRef"].apply(lambda x: 0 if pd.isnull(x) else len(ast.literal_eval(x)))
    df["outCitationsCount"] = df["outCitationsRef"].apply(lambda x: 0 if pd.isnull(x) else len(ast.literal_eval(x)))

    # TODO leave fields in
    df = df.drop(columns=["inCitationsRef", "outCitationsRef", "datasetId"])
    # df["inCitationsRef"] = df["inCitationsRef"].fillna({i: [] for i in df.index})  # empty List
    # df["outCitationsRef"] = df["outCitationsRef"].fillna({i: [] for i in df.index})  # empty List

    log.info("Set access type")
    df["openAccess"] = df["ee"].str.contains("\"@type\": \"oa\"|@type: oa", na=False)

    df = df.drop(columns=["ee"])

    log.info("Set venue")
    df["venue"] = df["venueRaw"].apply(dict_venues.get)
    df = df.drop(columns=["venueRaw"])

    log.info("Set authors")
    df["authors"] = df["authorsRaw"].apply(map_author)
    df = df.drop(columns=["authorsRaw"])

    df.to_csv(filename)


def analyse_cols(df_data: pd.DataFrame):
    empty_cols = []
    sparse_columns = ["pmid", "entities", "crossref", "cdrom", "cite", "note", "@publtype", "month",
                      "booktitle", "publisher", "school", "isbn", "series", "editor", "chapter",
                      "@cdate", "address"]

    for column in df_data.columns:
        if df_data[column].isnull().all():
            empty_cols.append(column)
        elif column in sparse_columns:
            log.info(f"Unique values in {column}:\n{df_data[column].unique()}")
    log.info(f"Empty columns: {empty_cols}")


def compare_cols(df_data: pd.DataFrame):
    df_data2 = df_data[["journal", "venue"]]
    subsetDataFrame2 = df_data2[df_data2['journal'] != df_data2["venue"]]
    log.info(subsetDataFrame2)

    df_data = df_data[["journal", "journalName"]]
    subsetDataFrame = df_data[df_data['journal'] != df_data["journalName"]]
    log.info(subsetDataFrame)


if __name__ == '__main__':
    # mode = "test"
    mode = "full"

    # requirements: at least 32GB RAM; full dataset as CSV
    # note#1: steps below are needed for preparation and have to be executed in a shell
    # note#2: during list_authors() you might have to stop your backend
    #   32GB RAM could be just not enough to run both at the same time

    # create test dataset with
    # "head -n 15003 d3_2021_12.csv > d3_11k.csv" in terminal to create test csv

    # split full dataset using
    # https://stackoverflow.com/questions/20721120/how-to-split-csv-files-as-per-number-of-rows-specified
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

    if not os.path.isfile(f"d3_1_test.csv"):
        df_test = load_dataset("d3_11k.csv")
        split = math.floor(len(df_test.index)/4)
        for i in range(1, 5):
            df_part = df_test.iloc[split*(i-1):split*i]
            df_part.to_csv(f"d3_test_{i}.csv")

    start = time.time()
    log.info(mode + " mode")
    for i in range(1, 5):
        gc.collect()
        log.info(f"Processing part {i}/4...")

        filename_venues = f"d3_venues_{mode}_{i}.csv"
        filename_authors = f"d3_authors_{mode}_{i}.csv"
        if not os.path.isfile(filename_venues) or not os.path.isfile(filename_authors):
            start_part = time.time()
            # analyse_cols(df_data)
            # compare_cols(df_data)
            log.info(f"Load dataset {i}/4")
            df_data = load_dataset(f"d3_{mode}_{i}.csv")
            list_venues(df_data, filename_venues)
            list_authors(df_data, filename_authors)
            del df_data

            log.info(f"Part {i}/4 took {(time.time()-start_part) / 60} min")
        gc.collect()

    combine_table(mode, "venues")
    combine_table(mode, "authors")
    post_table("venues", mode, 1000)
    post_table("authors", mode, 1000)
    df_venues = get_table("venues", mode, size=10000)
    df_authors = get_table("authors", mode, size=10000)
    log.info("Create dictionaries for id mappings")
    dict_venues = map_venue_name_to_id(df_venues)
    dict_authors = map_author_name_to_id(df_authors)
    del df_venues
    del df_authors
    gc.collect()

    for i in range(1, 5):
        gc.collect()
        log.info(f"Processing part {i}/4...")
        start_part = time.time()

        filename = f"d3_{mode}_edit_{i}.csv"
        if not os.path.isfile(filename):
            log.info(f"Load dataset {i}/4")
            df_data = load_dataset(f"d3_{mode}_{i}.csv")
            transform_papers(df_data, filename, dict_venues, dict_authors)
            del df_data

        log.info(f"Part {i}/4 took {(time.time() - start_part) / 60} min")
        gc.collect()

    post_papers(mode)

    log.info(f"Total time: {(time.time() - start)/60} min")
