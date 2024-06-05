import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def extract_bomb_summary_player_data(bomb_summary_player):
    detailed_data = []
    if bomb_summary_player is not None:
        for participant_id, details in bomb_summary_player.items():
            extracted_data = {
                'participant_ID': participant_id,
                'BOMBS_EXPLODED_CHAINED': details.get('BOMBS_EXPLODED', {}).get('CHAINED', 0),
                'BOMBS_EXPLODED_FIRE': details.get('BOMBS_EXPLODED', {}).get('FIRE', 0),
                'BOMBS_EXPLODED_STANDARD': details.get('BOMBS_EXPLODED', {}).get('STANDARD', 0),
                'BOMBS_DEFUSED_CHAINED': details.get('BOMBS_DEFUSED', {}).get('CHAINED', 0),
                'BOMBS_DEFUSED_FIRE': details.get('BOMBS_DEFUSED', {}).get('FIRE', 0),
                'BOMBS_DEFUSED_STANDARD': details.get('BOMBS_DEFUSED', {}).get('STANDARD', 0),
            }
            for bomb_type in ['CHAINED', 'FIRE', 'STANDARD']:
                for phase in ['B', 'R', 'G']:
                    key = f'PHASES_DEFUSED_{bomb_type}_{phase}'
                    extracted_data[key] = details.get('PHASES_DEFUSED', {}).get(bomb_type, {}).get(phase, 0)
            detailed_data.append(extracted_data)
    return detailed_data

def extract_individual_data(data, members, trial_id):
    indiv_data = []
    player_fields = ["TeammatesRescued", "TimesFrozen", "TextChatsSent", "FlagsPlaced",
                     "FiresExtinguished", "DamageTaken", "NumCompletePostSurveys",
                     "BombBeaconsPlaced", "BudgetExpended", "CommBeaconsPlaced"]
    for member in members:
        # Include trial_id in each member's data
        member_data = {"trial_id": trial_id, "participant_ID": member}
        for field in player_fields:
            field_data = data.get(field, {}) or {}
            # Ensure field_data is a dictionary before attempting to use .get on it
            member_data[field] = field_data.get(member, 0) if isinstance(field_data, dict) else 0
        indiv_data.append(member_data)
    return indiv_data

def extract_trial_summary_data(json_obj):
    team_data = {}
    trial_id = json_obj['msg'].get('trial_id', '')
    data = json_obj.get('data', {})
    team_data['trial_id'] = trial_id

    team_fields = ["StartTimestamp", "NumStoreVisits", "TotalStoreTime", "ASICondition",
                   "ExperimentName", "TeamScore", "MissionEndCondition", "TrialEndCondition",
                   "NumFieldVisits", "BombsTotal"]
    for field in team_fields:
        team_data[field] = data.get(field, "")

    bombs_exploded = data.get("BombsExploded")
    if bombs_exploded is not None:
        bombs_exploded_fields = ["EXPLODE_CHAINED_ERROR", "EXPLODE_FIRE", "EXPLODE_TIME_LIMIT", "EXPLODE_TOOL_MISMATCH", "TOTAL_EXPLODED"]
        for field in bombs_exploded_fields:
            team_data[field] = bombs_exploded.get(field, 0)
    else:
        for field in ["EXPLODE_CHAINED_ERROR", "EXPLODE_FIRE", "EXPLODE_TIME_LIMIT", "EXPLODE_TOOL_MISMATCH", "TOTAL_EXPLODED"]:
            team_data[field] = 0

    team_component_fields = ["TeammatesRescued", "TimesFrozen", "TextChatsSent", "FlagsPlaced",
                             "FiresExtinguished", "DamageTaken", "NumCompletePostSurveys",
                             "BombBeaconsPlaced", "BudgetExpended", "CommBeaconsPlaced"]
    for field in team_component_fields:
        field_data = data.get(field, {})
        team_data[field] = field_data.get("Team", 0) if isinstance(field_data, dict) else 0

    members = data.get("Members", [])
    indiv_data = extract_individual_data(data, members, trial_id)
    bomb_summary_player_data = extract_bomb_summary_player_data(data.get('BombSummaryPlayer', {}))
    for bomb_data in bomb_summary_player_data:
        for indiv in indiv_data:
            if indiv["participant_ID"] == bomb_data["participant_ID"]:
                indiv.update(bomb_data)

    return team_data, indiv_data

def process_metadata_files(metadata_dir_path, output_folder_path):
    # output_folder_path = Path('C:/Post-doc Work/ASIST Study 4/Processed_TrialSummary')  # Explicitly set the absolute path
    output_folder_path = Path(output_folder_path)
    output_folder_path.mkdir(parents=True, exist_ok=True)
    # print(f"Output folder path: {output_folder_path}")  # Debug print statement

    metadata_files = list(Path(metadata_dir_path).glob('*.metadata'))
    # print(f"Found {len(metadata_files)} metadata files.")  # Debug print statement

    for i, file_path in enumerate(tqdm(metadata_files), start=1):
        # print(f"Processing file {i}/{len(metadata_files)}: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                content = json.loads(line)
                msg_type = content['msg'].get('sub_type', '')
                if msg_type == 'Event:TrialSummary':
                    team_data, indiv_data = extract_trial_summary_data(content)
                    team_df = pd.DataFrame([team_data])
                    indiv_df = pd.DataFrame(indiv_data)

                    team_csv_path = output_folder_path / f"{file_path.stem}_TrialSummaryData_TeamLevel.csv"
                    indiv_csv_path = output_folder_path / f"{file_path.stem}_TrialSummaryData_IndivLevel.csv"

                    team_df.to_csv(team_csv_path, index=False)
                    indiv_df.to_csv(indiv_csv_path, index=False)

                    # print(f"Data extracted and saved to {team_csv_path} and {indiv_csv_path}")  # Debug print statement

                    break  # Stop after processing the first TrialSummary message

    # print("All files processed.")

if __name__ == "__main__":
    folder_path = 'E:\ASIST Study 4\Study4_MetadataFiles'
    process_metadata_files(folder_path)
