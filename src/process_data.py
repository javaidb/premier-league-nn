import pandas as pd
import os
import json
import re

from thefuzz import process
from unidecode import unidecode

path_to_data = "../data"
path_to_us = f"{path_to_data}/raw/understat/"
path_to_processed = f"{path_to_data}/processed/"
season_data_dir = f"{path_to_us}/season_data"
player_bank_data_dir = f"{path_to_us}/player_bank"
path_to_ext = f"{path_to_data}/raw/ext_src"


def create_player_data_summaries():

    import_file_name = "player_data_raw"

    player_data = {}
    szns = os.listdir(season_data_dir)

    for year in szns:

        df = pd.read_csv(f'{season_data_dir}/{year}/players/{import_file_name}.csv', encoding='cp1252')

        for _, row in df.iterrows():

            player_understat_id = f'{row[row.keys()[0]]}'
            row_dict = row[1:].to_dict()

            # Add all player data to season_data
            player_data.setdefault(player_understat_id, {}).setdefault('understat', {}).setdefault('season_data', {})
            player_data[player_understat_id]['understat']['season_data'][f'{year}'] = row_dict

            # Add unique player names to season_data
            player_data.setdefault(player_understat_id, {}).setdefault('understat', {}).setdefault('player_data', {}).setdefault('player_name', set())
            player_data[player_understat_id]['understat']['player_data']['player_name'].add(row['player_name'])
            
            # Add summarized team data to season_data
            player_data.setdefault(player_understat_id, {}).setdefault('understat', {}).setdefault('player_data', {}).setdefault('team_name_breakdown', [])

            found = False
            for entry in player_data[player_understat_id]['understat']['player_data']['team_name_breakdown']:
                if entry['team_name'] == row['team_title']:
                    entry['season'].append(year)  # Append the season to existing team
                    found = True
                    break
            if not found:
                player_data[player_understat_id]['understat']['player_data']['team_name_breakdown'].append({'team_name': row['team_title'], 'season': [year]})

    #Convert all sets to lists (for player_name)
    def replace_sets_with_lists(d):
        for key, value in d.items():
            if isinstance(value, set):
                d[key] = list(value)
            elif isinstance(value, dict):
                replace_sets_with_lists(value)

    replace_sets_with_lists(player_data)
    return player_data


def create_understat_id_mapping():
    import_file_name = "player_data_raw"

    understat_player_data = []
    player_ids = os.listdir(player_bank_data_dir)

    for player_id in player_ids:

        df = pd.read_csv(f'{player_bank_data_dir}/{player_id}/{import_file_name}.csv', encoding='cp1252')

        player_dict = {}
        seasons = []
        teamlist = []
        teams = set()

        for _, row in df.iterrows():

            player_id = row['id']
            player_name = row['player_name']
            seasons.append(row['season'])
            teams.add(row['team_title'])
            teamlist.append((row['team_title'], row['season']))

        player_dict = {
            'id': player_id,
            'full_name': player_name,
            'seasons_in_pl': seasons,
            'teams_in_pl': list(teams),
            'pl_team_breakdown': teamlist,
        }
        understat_player_data.append(player_dict)
    
    understat_player_df = pd.DataFrame(understat_player_data)
    return understat_player_df



def map_understat_to_fpl():
    import_file_name = "player_idlist"
    understat_mapping_file = "understat_player_to_id"

    threshold_score = 50

    understat_to_fpl_mapping = {}
    szns = os.listdir(path_to_ext)

    def clean_name(input_string):
        # Convert accented characters to ASCII
        normalized_string = unidecode(input_string)
        cleaned_string = re.sub(r'[^A-Za-z\s]', '', normalized_string) # Keep only letters and spaces
        return cleaned_string

    for fpl_season in szns:

        season_year = "20" + fpl_season[-2:]

        understat_ids = pd.read_csv(f'{path_to_processed}/{understat_mapping_file}.csv')
        filtered_szn = understat_ids[
            understat_ids['seasons_in_pl'].apply(lambda x: season_year in x)
        ]
        
        understat_player_names = [clean_name(x) for x in filtered_szn['full_name'].tolist()]

        for ind, row in filtered_szn.iterrows():
            understat_to_fpl_mapping.setdefault(str(row['id']), {})['player_name'] = row['full_name']

        fpl_ids = pd.read_csv(f'{path_to_ext}/{fpl_season}/{import_file_name}.csv')
        for ind, row in fpl_ids.iterrows():

            fpl_full_player_name_raw = row['first_name'] + " " + row['second_name']
            fpl_full_player_name_proc = clean_name(fpl_full_player_name_raw)
            fpl_id = row['id']

            try:
                best_match, score = process.extractOne(fpl_full_player_name_proc, understat_player_names, score_cutoff=threshold_score)
                understat_id = filtered_szn.iloc[understat_player_names.index(best_match)]['id']
                understat_player_name_raw = filtered_szn.iloc[understat_player_names.index(best_match)]['full_name']
                understat_player_name_proc = understat_player_names[understat_player_names.index(best_match)]

                if score < 90: # Just ignore findings if score too low
                    fpl_id = '-'
                    fpl_full_player_name_raw = '-'

                # See if another match already exists for this season
                index = next((i for i, d in enumerate(understat_to_fpl_mapping[str(understat_id)].setdefault('fpl', [])) if d['fpl_season'] == fpl_season), None)
                fpl_match_dict = {
                    'fpl_season': fpl_season,
                    'fpl_id': fpl_id,
                    'fpl_name_raw': fpl_full_player_name_raw,
                    'debug':{
                        'fpl_name_processed_for_match': fpl_full_player_name_proc,
                        'understat_name_processed_for_match': understat_player_name_proc,
                        'match_score': score
                    }
                }

                if index is not None:
                    # If entry already exists for this match for this season, only replace if better matching score
                    if understat_to_fpl_mapping[str(understat_id)].setdefault('fpl', [])[index]['debug']['match_score'] < score:
                        understat_to_fpl_mapping[str(understat_id)].setdefault('fpl', [])[index] = fpl_match_dict  # Replace the existing dictionary
                else:
                    understat_to_fpl_mapping[str(understat_id)].setdefault('fpl', []).append(fpl_match_dict)

            except:

                # See if another match already exists for this and only append if not
                index = next((i for i, d in enumerate(understat_to_fpl_mapping[str(understat_id)].setdefault('fpl', [])) if d['fpl_season'] == fpl_season), None)

                if index is None:
                    understat_to_fpl_mapping.setdefault(str(understat_id), {}).setdefault('fpl', []).append(
                        {
                            'fpl_season': fpl_season,
                            'fpl_id': '-',
                            'fpl_name_raw': '-',
                        }
                    )
    return understat_to_fpl_mapping


if __name__ == "__main__":
    
    player_data = create_player_data_summaries()
    export_file_name = "player_data_summary"
    with open(f'{path_to_processed}/{export_file_name}.json', 'w') as json_file:
        json.dump(player_data, json_file, indent=4)

    understat_player_df = create_understat_id_mapping()
    export_file_name = "understat_player_to_id"
    understat_player_df.to_csv(f'{path_to_processed}/{export_file_name}.csv', index=False)
    
    understat_to_fpl_mapping = map_understat_to_fpl()
    export_file_name = "understat_to_fpl_mapping"
    with open(f'{path_to_processed}/{export_file_name}.json', 'w') as json_file:
        json.dump(understat_to_fpl_mapping, json_file, indent=4)
