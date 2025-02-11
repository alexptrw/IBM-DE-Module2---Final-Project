import pandas as pd
import requests
from bs4 import BeautifulSoup


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    page = requests.get(url).text
    parsed_page = BeautifulSoup(page, 'html.parser')
    table = parsed_page.find_all('tbody')
    rows = table[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            rank = int(col[0].contents[0][:-1])
            country = col[1].find('a')['title'].strip()
            cap = float(col[2].contents[0][:-1])
            dict_data = {
                table_attribs[0]: rank,
                table_attribs[1]: country,
                table_attribs[2]: cap
            }

            df1 = pd.DataFrame(dict_data, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    return df
url ='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Rank', 'Bank Name', 'Market cap']
print(extract(url, table_attribs))