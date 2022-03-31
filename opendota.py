import requests as re
import json
import time
from tqdm import tqdm


def pull_from_opendota(season_id):
    data_filename = f"match_data{season_id}.json"
    try:
        with open(f"known_matches{season_id}.json", "r") as fh:
            ids = set(json.load(fh).values())
    except FileNotFoundError:
        raise f"There is no matches_data file for season ID {season_id}"
    try:
        with open(data_filename, "r") as fh:
            match_data = json.load(fh)
    except FileNotFoundError:
        match_data = []
    ids.discard(0)  # some games get entered in ld2l.gg with opendota match ID 0, which is invalid
    known_ids = set(match["match_id"] for match in match_data)
    new_ids = ids - known_ids
    
    if len(new_ids) == 0:
        print("There are no new matches_data to pull")
        return match_data
    
    new_match_data = []
    for match_id in tqdm(new_ids):
        response = re.get(f"https://api.opendota.com/api/matches/{match_id}")
        content = json.loads(response.content)
        new_match_data.append(content)
        # TODO: fancier rate limiting
        time.sleep(1.1)
    
    # we could maybe do something more with the separated new match data, but I can't think of anything,
    # so we just combine them
    match_data.extend(new_match_data)
    with open(data_filename, "w") as fh:
        json.dump(match_data, fh)
    
    return match_data


def desired_fields(match):
    # MATCH ID, Radiant Team, K/D/A, XPM, GPM, Dire Team, K/D/A, XPM, GPM
    match_id = match["match_id"]
    ids = [match["radiant_team_id"], match["dire_team_id"]]
    sums = [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]
    for p in match["players"]:
        team = p["player_slot"] // 128  # player team is indicated with slot number: radiant 0-127, dire 128-255
        for i, stat in enumerate(("kills", "deaths", "assists", "gold_per_min", "xp_per_min")):
            sums[team][i] += p[stat]
    return match_id, ids[0], *sums[0], ids[1], *sums[1]


def make_csv(matches, season_id):
    # I like how concise I can be here with python, but I'm not sure how readable this is
    header = "match_id,radiant_team_id,r_kills,r_deaths,r_assists,r_xpm,r_gpm," \
             "dire_team_id,d_kills,d_deaths,d_assists,d_xpm,d_gpm"
    match_data_strings = [",".join(str(mv) for mv in desired_fields(m)) for m in matches]
    with open(f"selected_data{season_id}.csv", "w") as fh:
        fh.write("\n".join((header, *match_data_strings)))


if __name__ == '__main__':
    matches_data = pull_from_opendota(31)
    make_csv(matches_data, 31)
