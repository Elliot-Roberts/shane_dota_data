import csv
import json
import time
from typing import TypeVar, Optional
from tqdm import tqdm
from bs4 import BeautifulSoup
import requests as re

T = TypeVar("T")


class RateLimitedPuller:
    def __init__(self, seconds, base_url=""):
        self.duration = seconds
        self.prev_pull = 0
        self.base_url = base_url
    
    def pull(self, url):
        now = time.time()
        time_since = now - self.prev_pull
        if time_since < self.duration:
            time.sleep(self.duration - time_since)
        response = re.get(self.base_url + url)
        self.prev_pull = time.time()
        return response


def scrape_ld2l_completed_matches(content: bytes) -> set[int]:
    """
    Scrape ld2l.gg for the list of match_data_rows for the specified season, returning only the IDs for the match_data_rows which have
    finished and been filled out as such on the site. Note: we determine whether a match is complete by checking for a
    <span> element in the html of each row, which ld2l uses to indicate the winner (it is a crown icon).
    
    :param content: content of the ld2l.gg match list page as a bytes object
    :return: set of ld2l IDs for the completed match_data_rows
    """
    # response = re.get(f"https://ld2l.gg/seasons/{season_id}/match_data_rows")
    soup = BeautifulSoup(content, "html.parser")
    table_rows = list(soup.tbody.children)
    middle_columns = [list(row.children)[1] for row in table_rows]
    completed_matches = set(int(td.a["href"][9:]) for td in middle_columns if td.span is not None)
    return completed_matches


def ld2l_to_opendota(content: bytes) -> int:
    """
    Get the corresponding OpenDota match ID for a given ld2l match ID.
    
    :param content: content of the ld2l.gg page for a specific match
    :return: OpenDota match ID posted on ld2l.gg, which is possibly 0 if the game was not played (forfeit etc.)
    """
    # response = re.get(f"https://ld2l.gg/matches/{content}")
    soup = BeautifulSoup(content, "html.parser")
    od_link = soup.select_one(".ld2l-result-description > a:nth-child(2)")['href']
    return int(od_link[33:])  # take only the ID from the URL


def desired_fields(match: dict) -> tuple:
    """
    Extract the desired data from a match data object returned by the OpenDota /match_data_rows API, namely:
    MATCH ID, Radiant Team, K, D, A, XPM, GPM, Dire Team, K, D, A, XPM, GPM
    
    :param match: decoded json object from the OpenDota /match_data_rows API
    :return: tuple of the match ID and the team id, kills, deaths, assists, xpm, and gpm for both teams
    """
    match_id = match["match_id"]
    ids = [match["radiant_team_id"], match["dire_team_id"]]
    sums = [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]
    for p in match["players"]:
        team = p["player_slot"] // 128  # player team is indicated with slot number: radiant 0-127, dire 128-255
        for i, stat in enumerate(("kills", "deaths", "assists", "gold_per_min", "xp_per_min")):
            sums[team][i] += p[stat]
    return match_id, ids[0], *sums[0], ids[1], *sums[1]


def save_csv(match_data_rows: list[tuple], out_file: str) -> None:
    """
    Write a list of the match data tuples to a .csv file.
    
    :param match_data_rows: list of tuples that are the rows of data to save
    :param out_file: name of file to write to
    """
    header = "match_id,radiant_team_id,r_kills,r_deaths,r_assists,r_xpm,r_gpm," \
             "dire_team_id,d_kills,d_deaths,d_assists,d_xpm,d_gpm"
    match_data_strings = [",".join(str(r)) for r in match_data_rows]
    with open(out_file, "w") as fh:
        fh.write("\n".join((header, *match_data_strings)))


def update_data_for_season(season_id: int, csv_filename: Optional[str] = None) -> None:
    """
    Gathers match data for the specified ld2l season ID, placing it in the optionally specified file or an automatically
    named one. Attempts to make as few network requests as possible, using both data from the provided csv file (if it
    exists) and a local cache of pairings of ld2l IDs with OpenDota IDs.

    :param season_id: ld2l season ID (shows up in the url)
    :param csv_filename: name of a csv file to save data to and optionally read for existing data if present
    """
    # Generate name for csv if none is given
    if csv_filename is None:
        csv_filename = f"selected_data{season_id}.csv"
    
    # Objects to keep track of how frequently we are pulling from the two sites
    ld2l_puller = RateLimitedPuller(seconds=15, base_url="https://ld2l.gg/")
    od_puller = RateLimitedPuller(seconds=1, base_url="https://api.opendota.com/api/")
    
    # This will be a dictionary with entries {ld2l-match-id: OpenDota-match-id}
    id_pairing_cache_file = f"ld2l_match_id_mapping_s{season_id}.json"
    try:
        with open(id_pairing_cache_file, "r") as fh:
            cached_id_pairings = json.load(fh)
    except FileNotFoundError:
        cached_id_pairings = {}
    cached_ld2l_ids = set(int(x) for x in cached_id_pairings.keys())
    cached_od_ids = set(cached_id_pairings.values())
    
    try:
        with open(csv_filename, "r", newline='') as fh:
            known_match_data = list(iter(csv.reader(fh)))[1:]  # skipping the header line
    except FileNotFoundError:
        known_match_data = []
    known_data_ids = set(int(row[0]) for row in known_match_data)  # the IDs of matches we already have data for
    
    # We do some checks that probably won't be important in the normal case
    cached_but_no_data = cached_od_ids - known_data_ids
    cached_but_no_data.discard(0)  # we won't have data on un-played games, but this is not a problem
    if len(cached_but_no_data) != 0:
        print("Warning: csv file missing data on IDs in cache. Pulling missing match data...")
        for missing_id in tqdm(cached_but_no_data):
            known_match_data.append(desired_fields(od_puller.pull(f"matches/{missing_id}").json()))
    data_but_no_cache = known_data_ids - cached_od_ids
    if len(data_but_no_cache) != 0:
        # this is most likely either a user error or the cache was modified without changing the csv
        print("Warning: csv file contains data on IDs not present in cache.")
    
    # Fetch ld2l IDs for matches with winners posted on ld2l.gg, and difference this list with what we have cached.
    currently_posted = scrape_ld2l_completed_matches(ld2l_puller.pull(f"seasons/{season_id}/matches").content)
    new_ld2l_ids = currently_posted - cached_ld2l_ids
    assert len(cached_ld2l_ids - currently_posted) == 0, "cache contains ld2l IDs not present on ld2l.gg"
    
    # We are heavily assuming that no ld2l matches will have the same OpenDota ID.
    # If that sort of thing happens we'd probably have to delete the caches and restart.
    # TODO: handle error states better
    new_pairings = {}
    if len(new_ld2l_ids) > 0:
        # Get the corresponding OpenDota ID for each ld2l ID and fetch the match data from OpenDota
        for ld2l_id in tqdm(new_ld2l_ids, "[pairing and pulling new matches]"):
            od_id = ld2l_to_opendota(ld2l_puller.pull(f"matches/{ld2l_id}").content)
            new_pairings[ld2l_id] = od_id
            if od_id != 0 and od_id not in known_data_ids:
                known_match_data.append(desired_fields(od_puller.pull(f"matches/{od_id}").json()))
    else:
        print("No new matches.")
    
    # Finally, update the cache and write the csv file
    with open(id_pairing_cache_file, "w") as fh:
        json.dump(cached_id_pairings | new_pairings, fh)
    
    header = ("match_id",
              "radiant_team_id", "r_kills", "r_deaths", "r_assists", "r_xpm", "r_gpm",
              "dire_team_id", "d_kills", "d_deaths", "d_assists", "d_xpm", "d_gpm")
    with open(csv_filename, "w", newline='') as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(known_match_data)


if __name__ == '__main__':
    update_data_for_season(33)
