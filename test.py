import pandas as pd
import requests
import json
from time import time
import openpyxl
from bs4 import BeautifulSoup


def postcode_csv(file_path, export=f"{int(time())}_postcodes"):
    # need to add download of postcode csv from ons geoportal
    # bs4 could grab <a href> and then use as variable with requests library to automate dl
    # need to remove read on each invocation of function
    pcd_df_main = pd.read_csv(
        file_path,
        skipinitialspace=True,
        low_memory=False,
    )
    pcd_df_parse = pcd_df_main[["pcd", "lat", "long"]].reset_index(drop=True)
    pcd_df_parse[["pc1", "pc2"]] = pcd_df_parse["pcd"].str.split(expand=True)
    # pcd_df_parse.to_csv(f"{export}.csv")

    # drop duplicates from pc1 as this level of granularity is not needed
    pcd_df_parse = pcd_df_parse.drop_duplicates(subset="pc1")
    # double check to see if these steps are necessary
    pcd_df_parse["pc1"] = pcd_df_parse["pcd"].str[:4]
    pcd_df_parse["pc2"] = pcd_df_parse["pcd"].str[3:8]
    # remove postcodes with erroneous long lat data
    pcd_df_parse = pcd_df_parse[pcd_df_parse.lat < 99]
    pcd_df_parse = pcd_df_parse.drop_duplicates(subset="pc1").reset_index(drop=True)
    pcd_df_parse["pc1"] = pcd_df_parse["pc1"].str.replace(" ", "")
    pcd_df_parse["pc2"] = pcd_df_parse["pc2"].str.replace(" ", "")
    pcd_df_parse["pcd"] = pcd_df_parse["pcd"].str.replace(" ", "")
    pcd_df_parse.to_csv(f"{export}_cut.csv")
    return pcd_df_parse


# postcode("ONSPD_NOV_2021_UK/Data/ONSPD_NOV_2021_UK.csv")


def download_data(
    url=0,
    headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko)"
    },
):
    url_list = []
    for row in postcode_csv(
        "ONSPD_NOV_2021_UK/Data/ONSPD_NOV_2021_UK.csv"
    ).itertuples():
        url_list.append(
            f'https://www.aldi.co.uk/api/store-finder/search?latitude={getattr(row, "lat")}&longitude={getattr(row, "long")}'
        )
    return url_list


print(download_data())

# result = requests.get(url, headers=headers)

# df = pd.json_normalize(result.json(), record_path="results")
# df = df.drop(
#     ["distance", "description", "openingTimes", "isFirstStore", "selectedPage"],
#     axis=1,
# ).drop_duplicates("code")
# df[["retailer", "location"]] = df["name"].str.split("-", 1, expand=True)
# df.drop(["name", "retailer"], axis=1, inplace=True)
# df


def main():

    # format dataframe and save to csv
    x = x[["pcd", "lat", "long"]].reset_index(drop=True)
    x[["pc1", "pc2"]] = x["pcd"].str.split(expand=True)
    x.to_csv("postcodes.csv")

    x["pc3"] = x["pcd"].str.replace(" ", "")

    # %%
    # drop duplicates from dataframe
    y = x.drop_duplicates(subset="pc1")
    y["pc1"] = y["pcd"].str[:4]
    y["pc2"] = y["pcd"].str[3:8]
    y = y[y.lat < 99]
    y = y.drop_duplicates(subset="pc1").reset_index(drop=True)
    y["pc1"] = y["pc1"].str.replace(" ", "")
    y["pc2"] = y["pc2"].str.replace(" ", "")
    y.to_csv("postcodes_cut.csv")
    y
    # z = y['pc3'].squeeze().str.len()
    # print(z)
    # if length of string in pc3 == 7
    # pc2 = pc1.split(4 characters in)
    # print(y.iloc[:,0])

    # %% [markdown]
    # ### Data download
    #  - Loop through postcode/lat/long to download Aldi store data
    #  - Drop duplicates

    # %%
    # Test payload
    url = "https://www.aldi.co.uk/api/store-finder/search?latitude=53.6064826&longitude=-2.4095537"
    payload = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    }
    result = requests.get(url, headers=headers).json()

    df = pd.json_normalize(result, record_path="results")
    df = df.drop(
        ["distance", "description", "openingTimes", "isFirstStore", "selectedPage"],
        axis=1,
    ).drop_duplicates("code")
    df[["retailer", "location"]] = df["name"].str.split("-", 1, expand=True)
    df.drop(["name", "retailer"], axis=1, inplace=True)
    df

    # %%
    mythings = []
    for row in y.itertuples():
        url = f'https://www.aldi.co.uk/api/store-finder/search?latitude={getattr(row, "lat")}&longitude={getattr(row, "long")}'
        mythings.append(url)

    pd.DataFrame(data=mythings)

    # %%
    dump = []

    # remove h, and try it later
    for h, g in enumerate(mythings):
        result = requests.get(g, headers=headers)
        dump.append(result.json())

    # %%
    test = pd.DataFrame(data=dump).dropna().reset_index()
    s1 = json.dumps(test["results"].to_dict())
    s1
    # test2 = pd.DataFrame(s1.results.apply(json.load))
    # df_final = pd.json_normalize(test.attributes.apply(json.loads), record_path='results')

    # %%
    test.columns

    # %%
    new_test = pd.DataFrame(dump)
    new_test = new_test.dropna(how="all", subset="results")

    # %%
    # df = pd.json_normalize(dump, record_path='results', errors='ignore')
    # df = df.drop(['distance','description','openingTimes','isFirstStore','selectedPage'],axis=1).drop_duplicates('code')
    # df[['retailer', 'location']] = df['name'].str.split('-', 1, expand=True)
    # df[['town','postcode']] = pd.DataFrame(df.address.tolist(),index=df.index)

    # df.drop(['name', 'retailer', 'address'],axis=1,inplace=True)
    # df.to_json('test.json')
    # df = df.reset_index(drop=True)
    # df

    ###############

    # for i in range(len(my_list)):
    #   print(my_list.iloc[i, 3], my_list.iloc[i, 4])

    # %%
    new_test.reset_index(drop=True)
    new_test

    # %%
    testme = pd.Series.to_frame(new_test["results"])
    testme = testme.explode("results", ignore_index=False)
    testme = testme.to_dict(orient="records")
    testme = pd.json_normalize(testme)
    testme = testme.drop_duplicates(subset=["results.code"]).reset_index(drop=True)
    # testme = testme.explode('results.address').reset_index(drop=True)

    xyz = pd.DataFrame(
        testme["results.address"].to_list(), columns=["town", "pcd"]
    ).reset_index(drop=True)
    testme.insert(1, "pcd", xyz["pcd"])
    testme.drop_duplicates(subset="results.code")
    testme.to_excel("output.xlsx")

    # %% [markdown]
    # ## Now we do a Lidl scraping

    # %%
    # help = (f'https://spatial.virtualearth.net/REST/v1/data/588775718a4b4312842f6dffb4428cff/Filialdaten-UK/Filialdaten-UK'
    # f'?$select=*,__Distance&$filter=Adresstyp%20eq%201'
    # f'&key=Argt0lKZTug_IDWKC5e8MWmasZYNJPRs0btLw62Vnwd7VLxhOxFLW2GfwAhMK5Xg'
    # f'&$format=json&jsonp=Microsoft_Maps_Network_QueryAPI_10&spatialFilter=nearby({y[1]},{y[2]},4.8307364999999995)')

    lidlthings = []
    for row in y.itertuples():
        url = (
            f"https://spatial.virtualearth.net/REST/v1/data/588775718a4b4312842f6dffb4428cff/Filialdaten-UK/Filialdaten-UK"
            f"?$select=*,__Distance&$filter=Adresstyp%20eq%201"
            f"&key=Argt0lKZTug_IDWKC5e8MWmasZYNJPRs0btLw62Vnwd7VLxhOxFLW2GfwAhMK5Xg"
            f'&$format=json&jsonp=Microsoft_Maps_Network_QueryAPI_10&spatialFilter=nearby({getattr(row, "lat")},{getattr(row, "long")},4.8307364999999995)'
        )
        lidlthings.append(url)

    pd.DataFrame(data=lidlthings)

    dump2 = []
    payload = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    }

    # remove h, and try it later
    for h, g in enumerate(lidlthings):
        result = requests.get(g, headers=headers)
        dump2.append(result.text)

    # %%
    dump2
    with open("response.txt", "w") as f:
        f.write(str(dump2))

    # %%
    dict1 = {}
    with open("response.txt", "r") as f:
        data = f.read()
        # print([f for f in f])
    print(type(data))

    # %%
    # Test payload
    url = "https://www.openingtimesin.uk/retailers/lidl/"
    payload = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    }
    result = requests.get(url, headers=headers)
    htmldoc = result.content
    soup = BeautifulSoup(htmldoc, "html.parser")

    # print([type(item) for item in list(soup.children)])
    # html = list(soup.children)[1]
    # print(html.children)

    # %%
    # content = soup.find_all('ul', {"class": "categories"})
    # lines = [span.get_text() for span in content]

    def data_search(tag):
        return tag.has_attr("data-search")

    content = soup.find_all(data_search)
    lines = [span.get_text() for span in content]
    lines

    # content = soup.find_all('div', {"class": "row group"})
    # el = content.find(href=True)
    # el
    # for job_element in content:
    #     # title_element = job_element.find('li')
    #     # print(title_element)
    #     print(job_element.text.strip(), end="\n"*2)

    # %%
    soup.select("#A > div:nth-child(2) > ul")
    # for x in content:
    #     print(x)
