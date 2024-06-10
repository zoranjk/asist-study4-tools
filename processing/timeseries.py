''' functions for processing time series data '''

import os
import json
import pandas as pd
import csv
from tqdm import tqdm

##################################
# functions for message extraction
##################################

def get_timestamp(json_obj):
    # Check for '@timestamp' at the root of the JSON object
    if '@timestamp' in json_obj:
        return json_obj['@timestamp']
    # If not found, fall back to 'timestamp' within the 'msg' object
    return json_obj.get('msg', {}).get('timestamp', '')


def extract_trial_data(json_obj):
    trial_metadata = json_obj.get('data', {}).get('metadata', {}).get('trial', {})
    extracted_data = {
        'trial_info_experiment_id': json_obj.get('msg', {}).get('experiment_id', ''),
        'trial_info_trial_id': json_obj.get('msg', {}).get('trial_id', ''),
        'trial_info_name': trial_metadata.get('name', ''),
        'trial_info_date': trial_metadata.get('date', ''),
        'trial_info_subjects': ', '.join(trial_metadata.get('subjects', [])),
        'trial_info_condition': trial_metadata.get('condition', ''),
        'trial_info_experiment_name': trial_metadata.get('experiment_name', ''),
        'trial_info_experiment_mission': trial_metadata.get('experiment_mission', ''),
        'trial_info_map_name': trial_metadata.get('map_name', ''),
    }
    return [extracted_data]


def extract_flocking_data(json_obj):
    timestamp = get_timestamp(json_obj)
    base_data = {
        'trial_id': json_obj.get('msg', {}).get('trial_id', ''),
        'experiment_id': json_obj.get('msg', {}).get('experiment_id', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'flocking_phase': json_obj.get('data', {}).get('phase', ''),
        'flocking_td': json_obj.get('data', {}).get('td', ''),
        'elapsed_milliseconds_global': json_obj.get('data', {}).get('elapsed_milliseconds_global', ''),
        'flocking_period': json_obj.get('data', {}).get('period', ''),
        'flocking_time_in_store': json_obj.get('data', {}).get('time_in_store', ''),
        'flocking_separation': json_obj.get('data', {}).get('flocking', {}).get('separation', ''),
        'flocking_cohesion': json_obj.get('data', {}).get('flocking', {}).get('cohesion', ''),
        'flocking_alignment': json_obj.get('data', {}).get('flocking', {}).get('alignment', ''),
        'elapsed_ms_field': json_obj.get('data', {}).get('elapsed_ms_field', ''),
        'flocking_visits_to_store': json_obj.get('data', {}).get('visits_to_store', ''),
    }
    rows = []
    distance_data = json_obj.get('data', {}).get('reconaissance', {}).get('distance', {})
    stationary_data = json_obj.get('data', {}).get('reconaissance', {}).get('stationary', {})
    for participant_id, distances in distance_data.items():
        row = base_data.copy()
        row['participant_id'] = participant_id
        row['straightline_distance'] = distances.get('straightline', '')
        row['incremental_distance'] = distances.get('incremental', '')
        row['stationary_time'] = stationary_data.get(participant_id, '')
        rows.append(row)
    summation_data = json_obj.get('data', {}).get('reconaissance', {}).get('summation', [])
    for summation in summation_data:
        row = base_data.copy()
        row['participant_id'] = ' & '.join(summation.get('nad', []))
        row['overlap'] = summation.get('overlap', 0)
        row['nonoverlap'] = summation.get('nonoverlap', 0)
        row['ratio'] = summation.get('ratio', 0)
        rows.append(row)
    return rows


def extract_player_state_data(json_obj):
    """Extracts data for Event:PlayerState messages."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'trial_id': json_obj.get('msg', {}).get('trial_id', ''),
        'experiment_id': json_obj.get('msg', {}).get('experiment_id', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'participant_id': json_obj.get('data', {}).get('participant_id', ''),
        'mission_timer': json_obj.get('data', {}).get('mission_timer', ''),
        'elapsed_milliseconds_global': json_obj.get('data', {}).get('elapsed_milliseconds_global', ''),
        'player_state_motion_x': json_obj.get('data', {}).get('motion_x', ''),
        'player_state_motion_y': json_obj.get('data', {}).get('motion_y', ''),
        'player_state_motion_z': json_obj.get('data', {}).get('motion_z', ''),
        'player_state_x': json_obj.get('data', {}).get('x', ''),
        'player_state_y': json_obj.get('data', {}).get('y', ''),
        'player_state_z': json_obj.get('data', {}).get('z', ''),
        'player_state_yaw': json_obj.get('data', {}).get('yaw', ''),
        'player_state_pitch': json_obj.get('data', {}).get('pitch', ''),
        'elapsed_milliseconds_stage': json_obj.get('data', {}).get('elapsed_milliseconds_stage', ''),
        'player_state_obs_id': json_obj.get('data', {}).get('obs_id', '')
    }
    return extracted_data


def extract_ui_click_data(json_obj):
    """Extracts data for Event:UIClick messages."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'trial_id': json_obj.get('msg', {}).get('trial_id', ''),
        'experiment_id': json_obj.get('msg', {}).get('experiment_id', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'UIClick_call_sign_code': json_obj.get('data', {}).get('additional_info', {}).get('call_sign_code', ''),
        'UIClick_meta_action': json_obj.get('data', {}).get('additional_info', {}).get('meta_action', ''),
        'participant_id': json_obj.get('data', {}).get('participant_id', ''),
        'UIClick_element_id': json_obj.get('data', {}).get('element_id', '')
    }
    return [extracted_data]  # Return a list containing a single dictionary for consistency


def extract_chat_data(json_obj):
    """Extracts data for Event:Chat messages with specific field names renamed."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'trial_id': json_obj.get('msg', {}).get('trial_id', ''),
        'experiment_id': json_obj.get('msg', {}).get('experiment_id', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'Chat_addressees': json_obj.get('data', {}).get('addressees', []),
        'Chat_sender': json_obj.get('data', {}).get('sender', ''),
        'Chat_text': json_obj.get('data', {}).get('text', '')
    }
    return [extracted_data]


def extract_communication_chat_data(json_obj):
    """Extracts data for Event:CommunicationChat messages with specific fields renamed."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'CommunicationChat_source': json_obj.get('msg', {}).get('source', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'CommunicationChat_environment': json_obj.get('data', {}).get('environment', ''),
        'CommunicationChat_recipients': json_obj.get('data', {}).get('recipients', []),
        'CommunicationChat_message_id': json_obj.get('data', {}).get('message_id', ''),
        'CommunicationChat_message': json_obj.get('data', {}).get('message', ''),
        'CommunicationChat_sender_id': json_obj.get('data', {}).get('sender_id', '')
    }
    return [extracted_data]


def extract_communication_environment_data(json_obj):
    """Extracts data for Event:CommunicationEnvironment messages with specific fields renamed."""
    timestamp = get_timestamp(json_obj)

    additional_info = json_obj.get('data', {}).get('additional_info', {})
    extracted_data = {
        'CommunicationEnvironment_sender_z': json_obj.get('data', {}).get('sender_z', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'CommunicationEnvironment_bomb_id': additional_info.get('bomb_id', ''),
        'CommunicationEnvironment_fuse_start_minute': additional_info.get('fuse_start_minute', ''),
        'CommunicationEnvironment_remaining_sequence': additional_info.get('remaining_sequence', ''),
        'CommunicationEnvironment_chained_id': additional_info.get('chained_id', ''),
        'CommunicationEnvironment_recipients': json_obj.get('data', {}).get('recipients', []),
        'CommunicationEnvironment_sender_type': json_obj.get('data', {}).get('sender_type', ''),
        'CommunicationEnvironment_message_id': json_obj.get('data', {}).get('message_id', ''),
        'CommunicationEnvironment_sender_x': json_obj.get('data', {}).get('sender_x', ''),
        'CommunicationEnvironment_message': json_obj.get('data', {}).get('message', ''),
        'CommunicationEnvironment_sender_y': json_obj.get('data', {}).get('sender_y', ''),
        'CommunicationEnvironment_sender_id': json_obj.get('data', {}).get('sender_id', '')
    }
    return [extracted_data]


def extract_tool_used_data(json_obj):
    """Extracts data for Event:ToolUsed messages with specific fields renamed."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'ToolUsed_target_block_x': json_obj.get('data', {}).get('target_block_x', ''),
        'ToolUsed_target_block_y': json_obj.get('data', {}).get('target_block_y', ''),
        'ToolUsed_target_block_z': json_obj.get('data', {}).get('target_block_z', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'ToolUsed_tool_type': json_obj.get('data', {}).get('tool_type', ''),
        'participant_id': json_obj.get('data', {}).get('participant_id', ''),
        'ToolUsed_target_block_type': json_obj.get('data', {}).get('target_block_type', '')
    }
    return [extracted_data]


def extract_object_state_change_data(json_obj):
    """Extracts data for Event:ObjectStateChange messages with specific fields renamed."""
    timestamp = get_timestamp(json_obj)

    curr_attributes = json_obj.get('data', {}).get('currAttributes', {})
    changed_attributes = json_obj.get('data', {}).get('changedAttributes', {})
    extracted_data = {
        'ObjectStateChange_sequence': curr_attributes.get('sequence', ''),
        'ObjectStateChange_fuse_start_minute': curr_attributes.get('fuse_start_minute', ''),
        'ObjectStateChange_active': curr_attributes.get('active', ''),
        'ObjectStateChange_outcome': curr_attributes.get('outcome', ''),
        'ObjectStateChange_triggering_entity': json_obj.get('data', {}).get('triggering_entity', ''),
        'ObjectStateChange_x': json_obj.get('data', {}).get('x', ''),
        'ObjectStateChange_y': json_obj.get('data', {}).get('y', ''),
        'ObjectStateChange_z': json_obj.get('data', {}).get('z', ''),
        'ObjectStateChange_id': json_obj.get('data', {}).get('id', ''),
        'ObjectStateChange_type': json_obj.get('data', {}).get('type', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', '')
    }
    return [extracted_data]


def extract_score_change_data(json_obj):
    """Extracts data for Event:ScoreChange messages including dynamic playerScores."""
    timestamp = get_timestamp(json_obj)

    # Basic information extraction
    base_data = {
        'teamScore': json_obj.get('data', {}).get('teamScore', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', '')
    }

    # Extract and create rows for each player score
    player_scores = json_obj.get('data', {}).get('playerScores', {})
    extracted_data = []
    for participant_id, score in player_scores.items():
        # Create a new row for each participant, incorporating base data and player-specific score
        row = base_data.copy()
        row['participant_id'] = participant_id
        row['playerScore'] = score
        extracted_data.append(row)

    return extracted_data


def extract_item_used_data(json_obj):
    """Extracts data for Event:ItemUsed messages with specific fields renamed."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'ItemUsed_target_x': json_obj.get('data', {}).get('target_x', ''),
        'ItemUsed_target_y': json_obj.get('data', {}).get('target_y', ''),
        'ItemUsed_target_z': json_obj.get('data', {}).get('target_z', ''),
        'ItemUsed_item_id': json_obj.get('data', {}).get('item_id', ''),
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'timestamp': timestamp,
        'participant_id': json_obj.get('data', {}).get('participant_id', ''),
        'ItemUsed_item_name': json_obj.get('data', {}).get('item_name', '')
    }
    return [extracted_data]


def extract_intervention_chat_data(json_obj):
    """Extracts data for Event:InterventionChat messages with specific fields renamed."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'InterventionChat_source': json_obj.get('msg', {}).get('source', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'InterventionChat_duration': json_obj.get('data', {}).get('duration', ''),
        'InterventionChat_receivers': json_obj.get('data', {}).get('receivers', []),
        'InterventionChat_response_options': json_obj.get('data', {}).get('response_options', []),
        'InterventionChat_id': json_obj.get('data', {}).get('id', ''),
        'InterventionChat_content': json_obj.get('data', {}).get('content', ''),
        'InterventionChat_explanation': json_obj.get('data', {}).get('explanation', {})
    }
    return [extracted_data]


def extract_intervention_chat_b_data(json_obj):
    """Extracts data for Intervention:Chat messages with specific fields renamed."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'InterventionChat_b_source': json_obj.get('msg', {}).get('source', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'InterventionChat_b_duration': json_obj.get('data', {}).get('duration', ''),
        'InterventionChat_b_receivers': json_obj.get('data', {}).get('receivers', []),
        'InterventionChat_b_response_options': json_obj.get('data', {}).get('response_options', []),
        'InterventionChat_b_id': json_obj.get('data', {}).get('id', ''),
        'InterventionChat_b_explanation': json_obj.get('data', {}).get('explanation', {}),
        'InterventionChat_b_content': json_obj.get('data', {}).get('content', '')
    }
    return [extracted_data]


def extract_intervention_response_data(json_obj):
    """Extracts data for Event:InterventionResponse messages with specified fields renamed."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'InterventionResponse_response_index': json_obj.get('data', {}).get('response_index', ''),
        'InterventionResponse_intervention_id': json_obj.get('data', {}).get('intervention_id', ''),
        'InterventionResponse_agent_id': json_obj.get('data', {}).get('agent_id', ''),
        'participant_id': json_obj.get('data', {}).get('participant_id', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', '')
    }
    return [extracted_data]


def extract_player_state_change_data(json_obj):
    """Extracts data for Event:PlayerStateChange messages with specified fields renamed."""
    timestamp = get_timestamp(json_obj)

    # Extracting nested 'changedAttributes' directly within the main dictionary for simplicity
    changed_attributes = json_obj.get('data', {}).get('changedAttributes', {})
    extracted_data = {
        'PlayerStateChanged_source_z': json_obj.get('data', {}).get('source_z', ''),
        'PlayerStateChanged_source_x': json_obj.get('data', {}).get('source_x', ''),
        'PlayerStateChanged_source_y': json_obj.get('data', {}).get('source_y', ''),
        'participant_id': json_obj.get('data', {}).get('participant_id', ''),
        'PlayerStateChanged_source_type': json_obj.get('data', {}).get('source_type', ''),
        'PlayerStateChanged_changedAttributes': str(changed_attributes),
        'PlayerStateChanged_is_frozen': json_obj.get('data', {}).get('currAttributes', {}).get('is_frozen', ''),
        'PlayerStateChanged_ppe_equipped': json_obj.get('data', {}).get('currAttributes', {}).get('ppe_equipped', ''),
        'PlayerStateChanged_health': json_obj.get('data', {}).get('currAttributes', {}).get('health', ''),
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'PlayerStateChanged_player_y': json_obj.get('data', {}).get('player_y', ''),
        'PlayerStateChanged_player_x': json_obj.get('data', {}).get('player_x', ''),
        'PlayerStateChanged_source_id': json_obj.get('data', {}).get('source_id', ''),
        'PlayerStateChanged_player_z': json_obj.get('data', {}).get('player_z', ''),
        'timestamp': timestamp
    }
    return [extracted_data]


def extract_player_sprinting_data(json_obj):
    """Extracts data for Event:PlayerSprinting messages."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'participant_id': json_obj.get('data', {}).get('participant_id', ''),
        'sprinting': json_obj.get('data', {}).get('sprinting', '')
    }
    return [extracted_data]


def extract_mission_state_data(json_obj):
    """Extracts data for Event:MissionState messages."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
        'mission_state': json_obj.get('data', {}).get('mission_state', ''),
        'state_change_outcome': json_obj.get('data', {}).get('state_change_outcome', '')
    }
    return [extracted_data]


def extract_team_budget_update_data(json_obj):
    """Extracts data for Event:TeamBudgetUpdate messages."""
    timestamp = get_timestamp(json_obj)

    extracted_data = {
        'team_budget': json_obj.get('data', {}).get('team_budget', ''),
        'timestamp': timestamp,
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', ''),
    }
    return [extracted_data]


def extract_mission_stage_transition_data(json_obj):
    """Extracts data for Event:MissionStageTransition messages."""
    timestamp = json_obj.get('@timestamp', '')

    extracted_data = {
        'timestamp': timestamp,
        'mission_stage': json_obj.get('data', {}).get('mission_stage', ''),
        'transitions_to_shop': json_obj.get('data', {}).get('transitionsToShop', ''),
        'transitions_to_field': json_obj.get('data', {}).get('transitionsToField', ''),
        'team_budget': json_obj.get('data', {}).get('team_budget', ''),
        'elapsed_milliseconds': json_obj.get('data', {}).get('elapsed_milliseconds', '')
    }
    return [extracted_data]


# Function to pre-scan files and determine all unique fieldnames
def pre_scan_for_fieldnames(folder_path):
    unique_keys = {
        'trial_id', 'experiment_id', 'timestamp', 'phase', 'td',
        'elapsed_milliseconds_global', 'period', 'time_in_store', 'separation',
        'cohesion', 'alignment', 'elapsed_ms_field', 'visits_to_store', 'teamScore', 'playerScore',
        'trial_info_experiment_id', 'trial_info_trial_id', 'trial_info_name', 'trial_info_date',
        'trial_info_subjects', 'trial_info_condition', 'trial_info_experiment_name',
        'trial_info_experiment_mission', 'trial_info_map_name', 'participant_id', 'straightline_distance',
        'incremental_distance', 'stationary_time', 'overlap', 'nonoverlap', 'ratio',
        'mission_stage', 'transitions_to_shop', 'transitions_to_field', 'team_budget', 'elapsed_milliseconds'
    }
    renamed_fields = {
        'Event:UIClick': {
            'meta_action': 'UIClick_meta_action',
            'element_id': 'UIClick_element_id',
        },
        'Event:PlayerState': {
            'x': 'player_state_x',
            'y': 'player_state_y',
            'z': 'player_state_z',
            'yaw': 'player_state_yaw',
            'pitch': 'player_state_pitch',
            'obs_id': 'player_state_obs_id'
        },
        'Measure:flocking': {
            'phase': 'flocking_phase',
            'td': 'flocking_td',
            'period': 'flocking_period',
            'time_in_store': 'flocking_time_in_store',
            'separation': 'flocking_separation',
            'cohesion': 'flocking_cohesion',
            'alignment': 'flocking_alignment',
            'visits_to_store': 'flocking_visits_to_store'
        },
        'Event:Chat': {
            'addressees': 'Chat_addressees',
            'sender': 'Chat_sender',
            'text': 'Chat_text'
        },
        'Event:CommunicationChat': {
            'source': 'CommunicationChat_source',
            'environment': 'CommunicationChat_environment',
            'recipients': 'CommunicationChat_recipients',
            'message_id': 'CommunicationChat_message_id',
            'message': 'CommunicationChat_message',
            'sender_id': 'CommunicationChat_sender_id',
        },
        'Event:CommunicationEnvironment': {
            'source': 'CommunicationEnvironment_source',
            'environment': 'CommunicationEnvironment_environment',
            'recipients': 'CommunicationEnvironment_recipients',
            'message_id': 'CommunicationEnvironment_message_id',
            'message': 'CommunicationEnvironment_message',
            'sender_id': 'CommunicationEnvironment_sender_id',
            'sender_x': 'CommunicationEnvironment_sender_x',
            'sender_y': 'CommunicationEnvironment_sender_y',
            'sender_z': 'CommunicationEnvironment_sender_z',
            'sender_type': 'CommunicationEnvironment_sender_type',
        },
        'Event:ToolUsed': {
            'target_block_x': 'ToolUsed_target_block_x',
            'target_block_y': 'ToolUsed_target_block_y',
            'target_block_z': 'ToolUsed_target_block_z',
            'tool_type': 'ToolUsed_tool_type',
            'target_block_type': 'ToolUsed_target_block_type',
        },
        'Event:ObjectStateChange': {
            'sequence': 'ObjectStateChange_sequence',
            'fuse_start_minute': 'ObjectStateChange_fuse_start_minute',
            'active': 'ObjectStateChange_active',
            'outcome': 'ObjectStateChange_outcome',
            'triggering_entity': 'ObjectStateChange_triggering_entity',
            'x': 'ObjectStateChange_x',
            'y': 'ObjectStateChange_y',
            'z': 'ObjectStateChange_z',
            'id': 'ObjectStateChange_id',
            'type': 'ObjectStateChange_type'
        },
        'Event:ItemUsed': {
            'target_x': 'ItemUsed_target_x',
            'target_y': 'ItemUsed_target_y',
            'target_z': 'ItemUsed_target_z',
            'item_id': 'ItemUsed_item_id',
            'item_name': 'ItemUsed_item_name'
        },
        'Event:InterventionChat': {
            'source': 'InterventionChat_source',
            'duration': 'InterventionChat_duration',
            'receivers': 'InterventionChat_receivers',
            'response_options': 'InterventionChat_response_options',
            'id': 'InterventionChat_id',
            'content': 'InterventionChat_content',
            'explanation': 'InterventionChat_explanation'
        },
        'Intervention:Chat': {
            'source': 'InterventionChat_b_source',
            'duration': 'InterventionChat_b_duration',
            'receivers': 'InterventionChat_b_receivers',
            'response_options': 'InterventionChat_b_response_options',
            'id': 'InterventionChat_b_id',
            'explanation': 'InterventionChat_b_explanation',
            'content': 'InterventionChat_b_content'
        },
        'Event:InterventionResponse': {
            'response_index': 'InterventionResponse_response_index',
            'intervention_id': 'InterventionResponse_intervention_id',
            'agent_id': 'InterventionResponse_agent_id'
        },
        'Event:PlayerStateChange': {
            'source_z': 'PlayerStateChanged_source_z',
            'source_x': 'PlayerStateChanged_source_x',
            'source_y': 'PlayerStateChanged_source_y',
            'source_type': 'PlayerStateChanged_source_type',
            'changedAttributes': 'PlayerStateChanged_changedAttributes',
            'is_frozen': 'PlayerStateChanged_is_frozen',
            'ppe_equipped': 'PlayerStateChanged_ppe_equipped',
            'health': 'PlayerStateChanged_health',
            'player_y': 'PlayerStateChanged_player_y',
            'player_x': 'PlayerStateChanged_player_x',
            'source_id': 'PlayerStateChanged_source_id',
            'player_z': 'PlayerStateChanged_player_z'
        }
    }
    for filename in tqdm(os.listdir(folder_path), desc='Pre-scanning metadata for field names'):
        if filename.endswith('.metadata'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    try:
                        content = json.loads(line)
                        msg_type = content.get('msg', {}).get('sub_type', '')
                        if msg_type:
                            if msg_type in renamed_fields:
                                for original, renamed in renamed_fields[msg_type].items():
                                    unique_keys.add(renamed)
                            else:
                                data_fields = content.get('data', {}).keys()
                                unique_keys.update(data_fields)
                    except json.JSONDecodeError:
                        continue
    return list(unique_keys)


# extract_and_save_data function to use pre-scanned fieldnames
def extract_and_write_time_series(metadata_unique_dir_path, processed_time_series_dir_path):
    os.makedirs(processed_time_series_dir_path, exist_ok=True)
    files = [f for f in os.listdir(metadata_unique_dir_path) if f.endswith('.metadata')]
    total_files = len(files)
    fieldnames = pre_scan_for_fieldnames(metadata_unique_dir_path)
    for file_index, filename in enumerate(tqdm(files), start=1):
        file_path = os.path.join(metadata_unique_dir_path, filename)
        output_file_name = filename.replace('.metadata', '_TimeSeriesData.csv')
        output_file_path = os.path.join(processed_time_series_dir_path, output_file_name)
        with open(file_path, 'r', encoding='utf-8') as file, open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for line in file:
                try:
                    content = json.loads(line)
                    msg_type = content.get('msg', {}).get('sub_type', '')
                    data_list = []
                    if msg_type == 'Event:PlayerState':
                        data_list = [extract_player_state_data(content)]
                    elif msg_type == 'trial':
                        data_list = extract_trial_data(content)
                    elif msg_type == 'Measure:flocking':
                        data_list = extract_flocking_data(content)
                    elif msg_type == 'Event:UIClick':
                        data_list = extract_ui_click_data(content)
                    elif msg_type == 'Event:Chat':
                        data_list = extract_chat_data(content)
                    elif msg_type == 'Event:CommunicationChat':
                        data_list = extract_communication_chat_data(content)
                    elif msg_type == 'Event:CommunicationEnvironment':
                        data_list = extract_communication_environment_data(content)
                    elif msg_type == 'Event:ToolUsed':
                        data_list = extract_tool_used_data(content)
                    elif msg_type == 'Event:ObjectStateChange':
                        data_list = extract_object_state_change_data(content)
                    elif msg_type == 'Event:ScoreChange':
                        data_list = extract_score_change_data(content)
                    elif msg_type == 'Event:ItemUsed':
                        data_list = extract_item_used_data(content)
                    elif msg_type == 'Event:InterventionChat':
                        data_list = extract_intervention_chat_data(content)
                    elif msg_type == 'Intervention:Chat':
                        data_list = extract_intervention_chat_b_data(content)
                    elif msg_type == 'Event:InterventionResponse':
                        data_list = extract_intervention_response_data(content)
                    elif msg_type == 'Event:PlayerStateChange':
                        data_list = extract_player_state_change_data(content)
                    elif msg_type == 'Event:PlayerSprinting':
                        data_list = extract_player_sprinting_data(content)
                    elif msg_type == 'Event:MissionState':
                        data_list = extract_mission_state_data(content)
                    elif msg_type == 'Event:TeamBudgetUpdate':
                        data_list = extract_team_budget_update_data(content)
                    elif msg_type == 'Event:MissionStageTransition':
                        data_list = extract_mission_stage_transition_data(content)
                    for data in data_list:
                        writer.writerow(data)
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON line in file: {filename}")
        # print(f"Processed {file_index}/{total_files} files ({(file_index / total_files) * 100:.2f}%)")


#########################################
# functions for cleaning time series csvs
#########################################

def estimate_elapsed_milliseconds_and_convert_timestamp(df):
    # Convert timestamp to a numeric value (e.g., UNIX timestamp)
    # First, ensure conversion to datetime is correct
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Convert datetime to UNIX timestamp in seconds
    df['timestamp_numeric'] = df['timestamp'].astype('int64') // 10 ** 9

    # Find the start row
    start_rows = df[df['mission_state'] == "Start"]
    if not start_rows.empty:
        start_row = start_rows.iloc[0]
        start_timestamp = start_row['timestamp']
        start_elapsed = start_row['elapsed_milliseconds'] if 'elapsed_milliseconds' in start_row and not pd.isnull(
            start_row['elapsed_milliseconds']) else 0

        # Calculate elapsed milliseconds for each row based on the 'start' timestamp
        df['estimated_elapsed_ms'] = (df['timestamp'] - start_timestamp).dt.total_seconds() * 1000 + start_elapsed
    else:
        print("Warning: No rows with 'mission_state' == 'Start' found.")
        df['estimated_elapsed_ms'] = pd.NA  # or set to 0 or any other default value as needed

    return df

def clean_time_series(processed_time_series_dir_path,
                      processed_time_series_cleaned_dir_path):
    os.makedirs(processed_time_series_cleaned_dir_path, exist_ok=True)

    # List of columns to remove
    columns_to_remove = [
        'metadata', 'study_version', 'MissionEndCondition', 'gold_pub_minute',
        'alignment_alpha_bravo_delta_stage', 'TimesFrozen', 'semantic_map', 'period',
        'motion_y', 'observers', 'BudgetExpended', 'date', 'dependencies', 'td',
        'BombBeaconsPlaced', 'TeammatesRescued', 'locations', 'group', 'y', 'id',
        'BombsExploded', 'door_x', 'experiment_name', 'list', 'observation', 'agent',
        'blocks', 'responder', 'open', 'Alpha_goal', 'gelp_results',
        'item_name', 'TeamsList', 'version', 'ParticipationCount', 'phase', 'z',
        'corresponding_observation_number', 'observation_number', 'item_id',
        'TotalStoreTime', 'status', 'obj', 'requester', 'currAttributes', 'Delta',
        'additional_info', 'NumFieldVisits', 'door_y', 'compact_extractions',
        'predictions', 'MissionVariant', 'exited_locations', 'request_time',
        'gold_results', 'OptionalSurveys', 'alignment_alpha_bravo_trial',
        'alignment_alpha_bravo_stage', 'type', 'separation', 'config', 'client_info',
        'transitionsToShop', 'entered_blocks', 'alignment_bravo_delta_stage',
        'playerScores', 'intervention_agents', 'experiment_mission', 'jag', 'priority',
        'corrected_text', 'ExperimentName', 'agent_name', 'group_number', 'alignment',
        'response_time_duration', 'TrialId', 'source', 'yaw', 'message',
        'semantic_map_name', 'created_ts', 'event_properties',
        'agent_type', 'exited_grid_location', 'left_blocks', 'exited_connections',
        'amount_discarded', 'Bravo_goal', 'stage_start', 'respond_stage', 'ExperimentId',
        'testbed_version', 'triggering_entity', 'equippeditemname', 'subjects',
        'response_index', 'gold_msg_id', 'bomb_id', 'requested_tool', 'gelp_msg_id',
        'CommBeaconsPlaced', 'experiment_date', 'PreTrialSurveys', 'subscribes',
        'response_tool', 'TrialName', 'tool_type', 'request_stage', 'connections',
        'callsign', 'gelp_pub_minute', 'cohesion', 'TeamId', 'DamageTaken',
        'LastActiveMissionTime', 'alignment_bravo_delta_trial', 'player_y', 'BombsTotal',
        'res_player_compliance_by_all_reqs', 'surveys', 'notes', 'motion_x',
        'map_block_filename', 'grid_location', 'records', 'Team', 'utterance_id',
        'FlagsPlaced', 'participant', 'team_id', 'door_z', 'ObjectStateChange_fuse_start_minute',
        'TextChatsSent', 'text', 'life', 'alignment_alpha_delta_trial', 'ParticipantId',
        'mission', 'map_name', 'state', 'experimenter', 'x', 'Bravo', 'measure_data',
        'condition', 'time_in_store', 'FiresExtinguished', 'player_x',
        'player_z', 'pitch', 'created', 'publishes', 'stage_end', 'owner', 'respond_time',
        'NumCompletePostSurveys', 'remaining_sequence', 'alignment_alpha_bravo_delta_trial',
        'complied_dyad_raw_balance', 'BombSummaryPlayer', 'changedAttributes', 'active',
        'Members', 'experiment_author', 'extractions',
        'dyad_compliance_by_requester_reqs', 'Delta_goal',
        'alignment_alpha_delta_stage', 'created_elapsed_time',
        'complied_dyad_raw', 'trial_number', 'currInv', 'PostTrialSurveys', 'swinging',
        'motion_z', 'study_number', 'Alpha', 'name' , 'SuccessfulBombDisposals', 'response',
        'interventions-given'
    ]

    # Iterate through each file in the directory
    for filename in tqdm(os.listdir(processed_time_series_dir_path)):
        if filename.endswith('.csv'):
            file_path = os.path.join(processed_time_series_dir_path, filename)
            df = pd.read_csv(file_path, low_memory=False)

            # Remove the unwanted columns
            df.drop(columns_to_remove, axis=1, inplace=True, errors='ignore')

            # Estimate elapsed_milliseconds and convert timestamp
            df = estimate_elapsed_milliseconds_and_convert_timestamp(df)

            # Save the updated dataframe to a new file in the output folder
            output_file_path = os.path.join(processed_time_series_cleaned_dir_path, filename)
            df.to_csv(output_file_path, index=False)
            # print(f'Processed {filename}')


##########################################
# functions to add profiles to time series
##########################################

# Function to process and save a file
def process_and_save_file(file_path, individuals_df, teams_df, output_folder):
    # Read the time series CSV, convert column names to lowercase, and 'participant_id', 'trial_id' to string
    time_series_df = pd.read_csv(file_path, low_memory=False)
    time_series_df.columns = time_series_df.columns.str.lower()
    time_series_df['participant_id'] = time_series_df['participant_id'].astype(str)
    time_series_df['trial_id'] = time_series_df['trial_id'].astype(str)

    # Merge with individuals data
    merged_df = pd.merge(time_series_df, individuals_df, on=['participant_id', 'trial_id'], how='left')

    # Merge with teams data
    final_df = pd.merge(merged_df, teams_df, on='trial_id', how='left')

    # Prepare new file path for the output folder
    new_file_name = os.path.basename(file_path).replace('.csv', '_Profiled.csv')
    new_file_path = os.path.join(output_folder, new_file_name)

    # Save to new file
    final_df.to_csv(new_file_path, index=False)
    # print(f"Processed and saved: {new_file_name}")

def add_profiles_to_time_series(processed_time_series_cleaned_dir_path,
                                individual_player_profiles_trial_measures_combined_file_path,
                                team_player_profiles_trial_measures_combined_file_path,
                                output_dir_path):

    # Ensure the output directory exists
    os.makedirs(output_dir_path, exist_ok=True)

    # Read the individual and team datasheets with specified columns
    individuals_df = pd.read_csv(individual_player_profiles_trial_measures_combined_file_path,
                                 usecols=['participant_ID', 'trial_id', 'Teamwork_potential_score',
                                          'Taskwork_potential_score', 'Teamwork_potential_category',
                                          'Taskwork_potential_category_liberal',
                                          'Taskwork_potential_category_conservative'])
    teams_df = pd.read_csv(team_player_profiles_trial_measures_combined_file_path,
                           usecols=['trial_id', 'Team_teamwork_potential_score', 'Team_teamwork_potential_category',
                                    'Team_taskwork_potential_score_liberal', 'Team_taskwork_potential_category_liberal',
                                    'Team_taskwork_potential_score_conservative',
                                    'Team_taskwork_potential_category_conservative',
                                    'geometric_alignment_allAttributes',    'physical_alignment_allAttributes',
                                    'algebraic_alignment_allAttributes',    'centroid_physical_alignment_allAttributes',
                                    'geometric_alignment_teamworkAttributes',    'physical_alignment_teamworkAttributes',
                                    'algebraic_alignment_teamworkAttributes',
                                    'centroid_physical_alignment_teamworkAttributes',
                                    'geometric_alignment_taskworkAttributes',   'physical_alignment_taskworkAttributes',
                                    'algebraic_alignment_taskworkAttributes',
                                    'centroid_physical_alignment_taskworkAttributes'
                                    ])

    # Convert the column names to lowercase for consistency
    individuals_df.columns = individuals_df.columns.str.lower()
    teams_df.columns = teams_df.columns.str.lower()

    # Rename individual player columns, excluding 'participant_id' and 'trial_id'
    individuals_df.columns = ['player_' + col if col not in ['participant_id', 'trial_id'] else col for col in
                            individuals_df.columns]

    # Convert 'participant_id' and 'trial_id' in both DataFrames to string to avoid type mismatch
    individuals_df['participant_id'] = individuals_df['participant_id'].astype(str)
    individuals_df['trial_id'] = individuals_df['trial_id'].astype(str)
    teams_df['trial_id'] = teams_df['trial_id'].astype(str)

    # Iterate over CSV files in the folder and process them
    for file_name in tqdm(os.listdir(processed_time_series_cleaned_dir_path)):
        if file_name.endswith('.csv'):
            file_path = os.path.join(processed_time_series_cleaned_dir_path, file_name)
            process_and_save_file(file_path, individuals_df, teams_df, output_dir_path)
