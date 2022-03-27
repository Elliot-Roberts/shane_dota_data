import requests as re
import json
import pickle
from bs4 import BeautifulSoup
import time
from tqdm import tqdm

# When true, read the first request from a file instead of the internet
TEST = True

season_id = 31  # 31 is for season 12
match_list_url = f"https://ld2l.gg/seasons/{season_id}/matches"

# This stuff is just so that I don't spam ld2l.gg while testing
if TEST:
    with open("cached_ld2l_matches.pickle", 'rb') as fh:
        response = pickle.load(fh)
else:
    # This is the normal way to get the match list page from ld2l.gg
    response = re.get(match_list_url)
    with open("cached_ld2l_matches.pickle", 'wb') as fh:
        pickle.dump(response, fh)

# Load previously retrieved match IDs from a file
try:
    with open(f"known_matches{season_id}.json", "r") as fh:
        old_posted = json.load(fh)
except FileNotFoundError:
    old_posted = {}

# Here we extract the ld2l match IDs (different from the OpenDota ones) from their match list page
# TODO: should validate to make sure the format of ld2l.gg hasn't changed
soup = BeautifulSoup(response.content, features="html.parser")
table_rows = list(soup.tbody.children)
middle_columns = [list(row.children)[1] for row in table_rows]
# We try to only collect the IDs for matches that have finished.
# If there is a span tag in the middle column then results have been posted for the match.
# (The span is the little crown they put next to the winning team)
# TODO: make sure the crown is a reliable indicator when in the middle of a season
posted = set(td.a["href"][9:] for td in middle_columns if td.span is not None)
new_matches = posted - old_posted.keys()
assert len(old_posted.keys() - posted) == 0  # no matches were removed from ld2l.gg

if len(new_matches) == 0:
    print("No new matches")
    exit(0)

# To get the OpenDota IDs we need to make extra requests
# We make a mapping between the ld2l and opendota ones
new_open_dota_ids = {}
for ld2l_id in tqdm(new_matches, desc="politely scraping ld2l.gg"):  # tqdm does the progress bar
    match_content = re.get(f"https://ld2l.gg/matches/{ld2l_id}").content
    match_soup = BeautifulSoup(match_content, features="html.parser")
    # TODO: again very brittle, just praying they leave their html exactly the same lol
    od_id = int(match_soup.find("p", class_="ld2l-result-description")("a")[1]["href"][33:])
    new_open_dota_ids[ld2l_id] = od_id
    time.sleep(1)

# Combine the new ones with the old and save to a file
all_matches = old_posted | new_open_dota_ids
assert len(all_matches) == len(old_posted) + len(new_open_dota_ids)
with open(f"known_matches{season_id}.json", "w") as fh:
    json.dump(all_matches, fh, sort_keys=True)
