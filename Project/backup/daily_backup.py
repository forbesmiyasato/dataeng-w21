import gzip
import json
from urllib.request import urlopen
from datetime import date

url = "http://rbi.ddns.net/getBreadCrumbData"

html = urlopen(url)

string = html.read().decode('utf-8')

data = json.loads(string)

today = date.today()

date_time = today.strftime("%b_%d_%Y")
file_name = '/home/miyasato/backup_' + date_time + '.json'

with gzip.open(file_name, 'wt', encoding="utf-8") as zipfile:
    json.dump(data, zipfile)
