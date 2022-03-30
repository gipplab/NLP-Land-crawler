import pandas as pd
import requests
import json
import time

def print_hi(name):
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def load_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "NLP-Scholar-Data-vJune2020/nlp-scholar-papers-vJune2020.txt", delimiter="\t", low_memory=False, header=0, index_col=0
    )


def transform_data(df):
    df = df[["AA title", "AA url", "AA doi", "AA publisher", "AA month of publication", "AA year of publication", "AA authors list", "AA first author full name", "NS venue name", "NS paper type"]]
    # df["datePublished"] = df["AA month of publication"] + "/01/" + df["AA year of publication"].astype(str)
    df["datePublished"] = "01/01/" + df["AA year of publication"].astype(str)
    df = df.drop(columns=["AA doi", "AA year of publication", "AA month of publication", "AA authors list", "AA first author full name", "NS venue name", "NS paper type", "AA url"])
    df = df.rename(columns={"AA title": "title", "AA paper id": "dbplId"})
    df["dblpId"] = "string"
    df["citationInfoTimestamp"] = "01/01/2022"
    df["absUrl"] = "test/test"
    df["pdfUrl"] = "url/url"
    df["doi"] = "test-doi"
    df["isStudentPaper"] = True
    df["isSharedTask"] = True
    df["atMainConference"] = True
    df["shortOrLong"] = "short"
    df["preProcessingGitHash"] = "hash-test"
    df["abstractExtractor"] = "grobid"
    df["typeOfPaper"] = "journal"
    df["abstractText"] = "no-abstract"


    # print(df)

    # data = df.head().to_dict(orient="index").keys()
    # print(data)
    # data = list(df.head().to_dict(orient="index").values())
    # print(data)
    # j = json.dumps(data)

    # return j
    return df

if __name__ == '__main__':
    print_hi('PyCharm')
    dataset = load_dataset()

    data = transform_data(dataset)

    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjp7Il9pZCI6IjYyMmY1MjUxMDViZGE2NmViYTQxNTEwZCIsImVtYWlsIjoidGVzdEB0ZXN0LmNvbSJ9LCJpYXQiOjE2NDcyNzUzODB9.kzHMdbuwVjMB8lE9x8hD54a5dwfi_BH64i0F_a1vp2I"
    my_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for row in data.iterrows():
        # print(row[1])
        paper = row[1].to_json()
        # print(paper)
        # data = list(df.head().to_dict(orient="index").values())
        # print(data)
        # js = json.dumps(data)
        response = requests.post('http://localhost:3000/api/v0/papers', headers=my_headers, data=paper)
        # print(response)
        # print(response.text)
        time.sleep(0.6)