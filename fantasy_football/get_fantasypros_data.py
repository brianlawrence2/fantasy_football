import requests
import pandas as pd 
from bs4 import BeautifulSoup

seasons = range(2012,2023)

for season in seasons:
    ADP_URL = f'https://www.fantasypros.com/nfl/adp/ppr-overall.php?year={season}'

    res = requests.get(ADP_URL)
    soup = BeautifulSoup(res.content, 'html.parser')
    table = soup.find('table')

    adp = pd.read_html(str(table))[0]
    adp['season'] = season

    print(adp.head)
    #adp.to_gbq(f'fantasypros.ppr_adp', project_id='fantasyvbd', if_exists='append')

