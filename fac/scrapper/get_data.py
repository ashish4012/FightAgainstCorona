import requests
import sqlite3
from bs4 import BeautifulSoup
from fac import config
from fac.core.database import FacDB

URL = config.MOHFW_URL
page = requests.get(URL)
conn = FacDB('SQLITE')

soup = BeautifulSoup(page.content, 'html.parser')
data_table = soup.find('table')
data_rows = data_table.find_all('tr')
for data_row in data_rows[1:-1]:
    data_cells = data_row.find_all('td')
    state = data_cells[1].text.strip()
    confirmed_cases_national = data_cells[2].text.strip()
    confirmed_cases_foreign = data_cells[3].text.strip()
    cured = data_cells[4].text.strip()
    death = data_cells[5].text.strip()

    if conn.get_state_id(state) is None:
        conn.insert_state(state)

    state_id = conn.get_state_id(state)

    demographics_data = {
        'state_id': state_id,
        'affected_nationals': confirmed_cases_national,
        'affected_foreigner': confirmed_cases_foreign,
        'cured': cured,
        'death': death,
    }

    conn.insert_demographics_data(demographics_data)
