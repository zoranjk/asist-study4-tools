''' functions for processing time series data '''

import os
import json
import pandas as pd
import csv
from pathlib import Path
import glob
import hashlib
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
    for filename in tqdm(os.listdir(folder_path), desc='  Pre-scanning metadata for field names'):
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


##################################
# functions for summarizing events
##################################

def record_trial_info_experiment_mission(df):
    """Record the entry in column 'trial_info_experiment_mission'."""
    if 'trial_info_experiment_mission' in df.columns and not df['trial_info_experiment_mission'].empty:
        return df['trial_info_experiment_mission'].iloc[0]
    return None


def count_communication_messages(df):
    """Record the number of entries in column 'communicationenvironment_message'."""
    return df['communicationenvironment_message'].notnull().sum()


def count_intervention_responses(df):
    """Record the number of entries in column 'interventionresponse_intervention_id'."""
    return df['interventionresponse_intervention_id'].notnull().sum()


def record_trial_info_condition(df):
    """Record the entry in column 'trial_info_condition'."""
    if 'trial_info_condition' in df.columns and not df['trial_info_condition'].empty:
        return df['trial_info_condition'].iloc[0]
    return None


def record_trial_info_subjects(df):
    """Record the entry in column 'trial_info_subjects'."""
    if 'trial_info_subjects' in df.columns and not df['trial_info_subjects'].empty:
        return df['trial_info_subjects'].iloc[0]
    return None


def count_playerstate_is_frozen(df):
    """Record the number of times 'playerstatechanged_is_frozen' shows 'TRUE' with datatype handling."""
    # Ensure the column is treated as string
    frozen_column = df['playerstatechanged_is_frozen'].astype(str).str.strip().str.upper()
    return (frozen_column == 'TRUE').sum()


def count_playerstate_ppe(df):
    """Count occurrences of 'TRUE' and 'FALSE' for playerstatechanged_ppe_equipped with datatype handling."""
    # Ensure the column is treated as string
    ppe_column = df['playerstatechanged_ppe_equipped'].astype(str).str.strip().str.upper()
    counts = {
        'ppe_equipped_true_count': (ppe_column == 'TRUE').sum(),
        'ppe_equipped_false_count': (ppe_column == 'FALSE').sum()
    }
    return counts


def count_sprinting_start(df):
    if 'sprinting' in df.columns:
        # Normalize the column data by trimming whitespace and converting to uppercase
        normalized_sprinting = df['sprinting'].astype(str).str.strip().str.upper()
        # Count how many times 'TRUE' appears, handling variations in case and whitespace
        sprinting_start_count = (normalized_sprinting == 'TRUE').sum()
        return {'sprinting_start_count': sprinting_start_count}
    else:
        return {'sprinting_start_count': 0}


def record_state_change_outcome_with_prefix(df):
    if 'state_change_outcome' in df.columns:
        # Convert the column to string type to ensure .str methods work
        outcomes = df['state_change_outcome'].astype(str).str.contains('MISSION_STOP_', na=False)
        unique_outcomes = df.loc[outcomes, 'state_change_outcome'].unique()
        if len(unique_outcomes) > 0:
            return unique_outcomes[0]  # Assuming you want the first matching outcome
        else:
            return None
    return None


def record_trial_info_map_name(df):
    """Record the entry in column 'trial_info_map_name'."""
    if 'trial_info_map_name' in df.columns and not df['trial_info_map_name'].empty:
        return df['trial_info_map_name'].iloc[0]
    return None


def record_trial_info_trial_id(df):
    """Record the entry in column 'trial_info_trial_id'."""
    if 'trial_info_trial_id' in df.columns and not df['trial_info_trial_id'].empty:
        return df['trial_info_trial_id'].iloc[0]
    return None


def count_toolused_target_block_type(df):
    """Count for each of the different types of entries in 'toolused_target_block_type'.
    Append 'toolused_on_' prefix to the output column names."""
    if 'toolused_target_block_type' in df.columns:
        # Count occurrences of each unique value
        counts = df['toolused_target_block_type'].value_counts().to_dict()
        # Create a new dictionary with modified keys
        modified_counts = {f'toolused_on_{key}': value for key, value in counts.items()}
        return modified_counts
    return {}


def count_tool_usage(df):
    """Count occurrences of each unique tool type in 'toolused_tool_type' column.
    Prepend 'toolused_count_' to each unique value as the prefix for output column names."""
    if 'toolused_tool_type' in df.columns:
        # Count occurrences of each unique value
        counts = df['toolused_tool_type'].value_counts().to_dict()
        # Create a new dictionary with modified keys
        modified_counts = {f'toolused_count_{key}': value for key, value in counts.items()}
        return modified_counts
    return {}


def max_flocking_visits(df):
    """Find the maximum number of flocking visits to the store."""
    if 'flocking_visits_to_store' in df.columns:
        max_store_visits = df['flocking_visits_to_store'].max()
    else:
        max_store_visits = 0
    return max_store_visits


def count_uiclick_element_id(df):
    if 'uiclick_element_id' in df:
        counts = df['uiclick_element_id'].value_counts().to_dict()
        return {f'uiclick_element_id_{key}': value for key, value in counts.items()}
    else:
        return {}


def record_trial_info_subjects(df):
    if 'trial_info_subjects' in df and not df['trial_info_subjects'].isnull().all():
        return {'trial_info_subjects': df['trial_info_subjects'].dropna().iloc[0]}
    else:
        return {'trial_info_subjects': None}


def count_communicationchat_source(df):
    if 'communicationchat_source' in df:
        counts = df['communicationchat_source'].value_counts().to_dict()
        return {f'communicationchat_source_id_{key}': value for key, value in counts.items()}
    else:
        return {}


def count_interventionchat_b_id(df):
    return {'interventionchat_b_count': df['interventionchat_b_id'].notnull().sum()}


def record_team_budget_lowest(df):
    if 'team_budget' in df:
        return {'team_budget_lowest': df['team_budget'].min()}
    return {'team_budget_lowest': None}


def count_objectstatechange_outcome_by_type(df):
    if 'objectstatechange_outcome' in df.columns and 'objectstatechange_type' in df.columns:
        combined = df.groupby(['objectstatechange_type', 'objectstatechange_outcome']).size()
        return {f'objectstatechange_outcome_count_{idx[0]}_{idx[1]}': count for idx, count in combined.items()}
    return {}


def count_chat_sender(df):
    if 'chat_sender' in df:
        counts = df['chat_sender'].value_counts().to_dict()
        return {f'chat_sender_count_{key}': value for key, value in counts.items()}
    else:
        return {}


def count_uiclick_meta_action(df):
    if 'uiclick_meta_action' in df:
        counts = df['uiclick_meta_action'].value_counts().to_dict()
        return {f'uiclick_meta_action_count_{key}': value for key, value in counts.items()}
    else:
        return {}


def count_communicationchat_message(df):
    return {'communicationchat_message_count': df['communicationchat_message'].notnull().sum()}


def record_max_flocking_time_in_store(df):
    if 'flocking_time_in_store' in df:
        return {'max_flocking_time_in_store': df['flocking_time_in_store'].max()}
    return {'max_flocking_time_in_store': None}


def count_playerstatechanged_health(df):
    return {'playerstatechanged_health_count': df['playerstatechanged_health'].notnull().sum()}


def find_highest_score_per_participant(df):
    if 'playerscore' in df.columns and 'participant_id' in df.columns:
        highest_scores = df.groupby('participant_id')['playerscore'].max()
        return {f'{participant_id}_highest_score': score for participant_id, score in highest_scores.items()}
    return {}


def count_interventionchat_b_source(df):
    return {'interventionchat_b_source_count': df['interventionchat_b_source'].notnull().sum()}


def retain_one_entry_for_columns(df, columns):
    # Assumes all rows have the same value for each of these columns, so we take the first non-null value
    retained_entries = {}
    for column in columns:
        if column in df.columns and not df[column].isnull().all():
            retained_entries[column] = df[column].dropna().iloc[0]
        else:
            retained_entries[column] = None
    return retained_entries


def process_file(filepath):
    """Process each file and generate summary."""
    df = pd.read_csv(filepath, low_memory=False)

    # Call the function and store its return value
    mission_state_change_outcome = record_state_change_outcome_with_prefix(df)

    summary = {
        **count_tool_usage(df),
        'Max_Store_Visits_flocking': max_flocking_visits(df),
        **count_playerstate_ppe(df),
        'Trial_Info_Experiment_Mission': record_trial_info_experiment_mission(df),
        'Communication_Messages_Count': count_communication_messages(df),
        'Intervention_Responses_Count': count_intervention_responses(df),
        'Trial_Info_Condition': record_trial_info_condition(df),
        **record_trial_info_subjects(df),  # Ensure this isn't duplicating with the line below
        'Trial_Info_Subjects': record_trial_info_subjects(df)['trial_info_subjects'],  # Adjust based on actual function return
        'PlayerState_Is_Frozen_Count': count_playerstate_is_frozen(df),
        'Mission_State_Change_Outcome': mission_state_change_outcome,  # Directly use the returned value
        'Trial_Info_Map_Name': record_trial_info_map_name(df),
        'trial_ID': record_trial_info_trial_id(df),  # Ensure key consistency (Trial_ID vs trial_id)
        **count_toolused_target_block_type(df),
        **count_uiclick_element_id(df),
        **count_communicationchat_source(df),
        **count_sprinting_start(df),
        **count_interventionchat_b_id(df),
        **record_team_budget_lowest(df),
        **count_objectstatechange_outcome_by_type(df),
        **count_chat_sender(df),
        **count_uiclick_meta_action(df),
        **count_communicationchat_message(df),
        **record_max_flocking_time_in_store(df),
        **count_playerstatechanged_health(df),
        **find_highest_score_per_participant(df),
        **count_interventionchat_b_source(df),
        **retain_one_entry_for_columns(df, [
            'team_teamwork_potential_score', 'team_teamwork_potential_category',
            'team_taskwork_potential_score_liberal', 'team_taskwork_potential_category_liberal',
            'team_taskwork_potential_score_conservative', 'team_taskwork_potential_category_conservative',
            'geometric_alignment_allattributes', 'physical_alignment_allattributes',
            'algebraic_alignment_allattributes', 'centroid_physical_alignment_allattributes',
            'geometric_alignment_teamworkattributes', 'physical_alignment_teamworkattributes',
            'algebraic_alignment_teamworkattributes',
            'centroid_physical_alignment_teamworkattributes',
            'geometric_alignment_taskworkattributes', 'physical_alignment_taskworkattributes',
            'algebraic_alignment_taskworkattributes',
            'centroid_physical_alignment_taskworkattributes'
        ])
    }

    summary_for_csv = {key: value if not isinstance(value, dict) else json.dumps(value) for key, value in summary.items()}
    summary_df = pd.DataFrame([summary_for_csv])
    return summary_df


def summarize_events(processed_time_series_cleaned_profiled_dir_path,
                     output_dir_path):
    # input_dir = Path('C:/Post-doc Work/ASIST Study 4/Processed_TimeSeries_CSVs_Cleaned_Profiled')
    # output_dir = Path('C:/Post-doc Work/ASIST Study 4/Processed_TrialSummary_Output_CSVs')
    # output_dir.mkdir(exist_ok=True)
    output_dir_path = Path(output_dir_path)
    output_dir_path.mkdir(exist_ok=True)
    # os.makedirs(output_dir_path, exist_ok=True)
    processed_time_series_cleaned_profiled_dir_path = Path(processed_time_series_cleaned_profiled_dir_path)

    for filepath in tqdm(processed_time_series_cleaned_profiled_dir_path.glob('*.csv')):
        # Modify the filename by replacing the suffix
        new_filename = filepath.name.replace('_TimeSeriesData_Profiled', '_TrialSummary_Profiled')
        output_filename = output_dir_path / new_filename

        summary_df = process_file(filepath)  # summary is already a DataFrame now
        summary_df.to_csv(output_filename, index=False)  # Save the DataFrame directly
        # print(f'Summary saved to {output_filename}')


################################
# functions to collate summaries
################################

def collate_summaries(processed_trial_summary_dir_path,
                      output_file_path):
    # Use glob to list all CSV files in the source directory
    csv_files = glob.glob(os.path.join(processed_trial_summary_dir_path, "*.csv"))

    # Initialize an empty list to store dataframes
    df_list = []

    # Loop through the list of csv files
    for csv_file in csv_files:
        # Read the current CSV file and append it to the list
        df = pd.read_csv(csv_file)
        df_list.append(df)

    # Concatenate all dataframes in the list into one
    combined_df = pd.concat(df_list, ignore_index=True)

    # Save the combined dataframe to a new CSV file
    combined_df.to_csv(output_file_path, index=False)

    # print(f'Combined CSV saved to: {output_file_path}')

#############################################
# functions for trial summary post processing
#############################################

def remove_specified_columns(dataframe, columns_to_remove):
    """
    Removes specified columns from the DataFrame if they exist.

    :param dataframe: The pandas DataFrame from which to remove columns.
    :param columns_to_remove: A list of column names to remove from the DataFrame.
    """
    # Ensures only existing columns are attempted to be removed, avoiding KeyErrors
    existing_columns = [col for col in columns_to_remove if col in dataframe.columns]
    dataframe.drop(columns=existing_columns, inplace=True, errors='ignore')


def sum_tool_usage(dataframe, new_column, columns_to_sum):
    """
    Sums the values of specified columns in a dataframe and creates a new column with the result.

    :param dataframe: The dataframe to modify.
    :param new_column: The name of the new column to be created.
    :param columns_to_sum: A list of column names whose values are to be summed.
    """
    dataframe[new_column] = dataframe[columns_to_sum].sum(axis=1)


def rename_column(dataframe, old_column, new_column):
    """
    Renames a column in the dataframe.

    :param dataframe: The dataframe to modify.
    :param old_column: The name of the column to be renamed.
    :param new_column: The new name for the column.
    """
    dataframe.rename(columns={old_column: new_column}, inplace=True)


def create_columns_based_on_criteria(dataframe, task_descriptions):
    """
    Creates new columns based on specific criteria outlined in task descriptions.
    """
    for desc in task_descriptions:
        column_name = desc['new_column']
        columns_to_sum = desc['columns_to_sum']
        sum_tool_usage(dataframe, column_name, columns_to_sum)


def calculate_score_extremes(dataframe, suffix='_highest_score'):
    """
    Calculates the maximum and minimum scores for each row across columns with a specific suffix.
    """
    score_columns = [col for col in dataframe.columns if col.endswith(suffix)]
    dataframe['player_score_max'] = dataframe[score_columns].max(axis=1)
    dataframe['player_score_min'] = dataframe[score_columns].min(axis=1)


def process_csv(input_filepath, output_filepath):
    """
    Processes the CSV file according to the specified tasks.

    :param input_filepath: The path to the input CSV file.
    :param output_filepath: The path where the output CSV file will be saved.
    """
    # Load the CSV file into a DataFrame
    df = pd.read_csv(input_filepath)

    # Define columns to remove based on suffixes and prefixes
    columns_with_suffix = [col for col in df.columns if col.endswith('_highest_score')]
    columns_with_prefix = [col for col in df.columns if col.startswith('chat_sender_count_')]

    sum_tool_usage(df, 'Wirecutters_Used', [
        'toolused_count_WIRECUTTERS_GREEN',
        'toolused_count_WIRECUTTERS_RED',
        'toolused_count_WIRECUTTERS_BLUE'
    ])
    sum_tool_usage(df, 'toolused_on_beacon_bomb', [
        'toolused_on_block_beacon_bomb',
        'toolused_on_asistmod:block_beacon_bomb'
    ])
    sum_tool_usage(df, 'toolused_on_beacon_hazard', [
        'toolused_on_block_beacon_hazard',
        'toolused_on_asistmod:block_beacon_hazard'
    ])
    sum_tool_usage(df, 'toolused_on_bomb_chained', [
        'toolused_on_asistmod:block_bomb_chained',
        'toolused_on_block_bomb_chained'
    ])
    sum_tool_usage(df, 'toolused_on_bomb_standard', [
        'toolused_on_asistmod:block_bomb_standard',
        'toolused_on_block_bomb_standard'
    ])

    sum_tool_usage(df, 'toolused_on_bomb_fire', [
        'toolused_on_asistmod:block_bomb_fire',
        'toolused_on_block_bomb_fire'
    ])

    rename_column(df, 'toolused_on_asistmod:block_fire_custom', 'toolused_on_fire')

    rename_column(df, 'toolused_on_asistmod:block_bomb_disposer', 'toolused_on_bomb_disposer')

    rename_column(df, 'trial_ID', 'trial_id')

    rename_column(df, 'interventionchat_b_count', 'interventionchat_count')

    ui_click_columns = [
        'uiclick_element_id_2_1_+', 'uiclick_element_id_1_1_+', 'uiclick_element_id_0_0_+',
        'uiclick_element_id_1_0_+', 'uiclick_element_id_0_2_+', 'uiclick_element_id_3_0_+',
        'uiclick_element_id_2_2_+', 'uiclick_element_id_4_0_+', 'uiclick_element_id_6_1_+',
        'uiclick_element_id_LeaveStoreButton', 'uiclick_element_id_7_0_+',
        'uiclick_element_id_MissionBriefCloseButton', 'uiclick_element_id_0_2_-',
        'uiclick_element_id_6_0_+', 'uiclick_element_id_7_2_+', 'uiclick_element_id_1_2_-',
        'uiclick_element_id_1_2_+', 'uiclick_element_id_4_2_+', 'uiclick_element_id_3_2_+',
        'uiclick_element_id_3_1_+', 'uiclick_element_id_2_0_+', 'uiclick_element_id_0_1_+',
        'uiclick_element_id_4_2_-', 'uiclick_element_id_3_2_-', 'uiclick_element_id_5_2_+',
        'uiclick_element_id_2_2_-', 'uiclick_element_id_4_1_+', 'uiclick_element_id_5_1_+',
        'uiclick_element_id_6_2_+', 'uiclick_element_id_7_1_+', 'uiclick_element_id_flag_Delta_0',
        'uiclick_element_id_2_0_-', 'uiclick_element_id_1_1_-', 'uiclick_element_id_2_1_-',
        'uiclick_meta_action_count_PLANNING_FLAG_UPDATE', 'uiclick_element_id_7_0_-',
        'uiclick_meta_action_count_PLANNING_FLAG_PLACED', 'uiclick_element_id_delete-button',
        'uiclick_element_id_flag_Bravo_3', 'uiclick_element_id_flag_Bravo_2',
        'uiclick_element_id_flag_Bravo_1', 'uiclick_element_id_flag_Bravo_0',
        'uiclick_meta_action_count_UNDO_PLANNING_FLAG_PLACED', 'uiclick_element_id_8_1_+',
        'uiclick_element_id_5_0_-', 'uiclick_element_id_5_0_+', 'uiclick_element_id_8_2_+',
        'uiclick_element_id_8_0_+', 'uiclick_element_id_3_1_-', 'uiclick_element_id_0_0_-',
        'uiclick_element_id_4_0_-', 'uiclick_element_id_6_0_-', 'uiclick_element_id_5_1_-',
        'uiclick_element_id_4_1_-', 'uiclick_element_id_0_1_-', 'uiclick_element_id_6_1_-',
        'uiclick_element_id_7_2_-', 'uiclick_element_id_3_0_-', 'uiclick_element_id_6_2_-',
        'uiclick_element_id_8_1_-', 'uiclick_element_id_5_2_-', 'uiclick_element_id_7_1_-',
        'uiclick_element_id_8_2_-', 'uiclick_element_id_8_0_-'
    ]

    task_descriptions = [
        {
            'new_column': 'defused_bombs_count',
            'columns_to_sum': [
                'objectstatechange_outcome_count_block_bomb_chained_DEFUSED',
                'objectstatechange_outcome_count_block_bomb_fire_DEFUSED',
                'objectstatechange_outcome_count_block_bomb_standard_DEFUSED'
            ]
        },
        {
            'new_column': 'defused_disposer_bombs_count',
            'columns_to_sum': [
                'objectstatechange_outcome_count_block_bomb_chained_DEFUSED_DISPOSER',
                'objectstatechange_outcome_count_block_bomb_standard_DEFUSED_DISPOSER',
                'objectstatechange_outcome_count_block_bomb_fire_DEFUSED_DISPOSER'
            ]
        },

        {
            'new_column': 'exploded_bombs_count',
            'columns_to_sum': [
                'objectstatechange_outcome_count_block_bomb_chained_EXPLODE_TIME_LIMIT',
                'objectstatechange_outcome_count_block_bomb_fire_EXPLODE_TIME_LIMIT',
                'objectstatechange_outcome_count_block_bomb_standard_EXPLODE_TIME_LIMIT',
                'objectstatechange_outcome_count_block_bomb_chained_EXPLODE_TOOL_MISMATCH',
                'objectstatechange_outcome_count_block_bomb_fire_EXPLODE_TOOL_MISMATCH',
                'objectstatechange_outcome_count_block_bomb_standard_EXPLODE_TOOL_MISMATCH',
                'objectstatechange_outcome_count_block_bomb_chained_EXPLODE_CHAINED_ERROR',
                'objectstatechange_outcome_count_block_bomb_standard_EXPLODE_FIRE',
                'objectstatechange_outcome_count_block_bomb_fire_EXPLODE_FIRE',
                'objectstatechange_outcome_count_block_bomb_chained_EXPLODE_FIRE'
            ]
        },

        {
            'new_column': 'triggered_bombs_count',
            'columns_to_sum': [
                'objectstatechange_outcome_count_block_bomb_standard_TRIGGERED',
                'objectstatechange_outcome_count_block_bomb_fire_TRIGGERED',
                'objectstatechange_outcome_count_block_bomb_chained_TRIGGERED'
            ]
        },

        {
            'new_column': 'triggered_advance_seq_bombs_count',
            'columns_to_sum': [
                'objectstatechange_outcome_count_block_bomb_fire_TRIGGERED_ADVANCE_SEQ',
                'objectstatechange_outcome_count_block_bomb_standard_TRIGGERED_ADVANCE_SEQ',
                'objectstatechange_outcome_count_block_bomb_chained_TRIGGERED_ADVANCE_SEQ'
            ]
        }
    ]

    create_columns_based_on_criteria(df, task_descriptions)

    flags_columns = [
        # Delta flags
        *[f'uiclick_element_id_flag_Delta_{i}' for i in range(16)],
        # Bravo flags
        *[f'uiclick_element_id_flag_Bravo_{i}' for i in range(23)],
        # Alpha flags
        *[f'uiclick_element_id_flag_Alpha_{i}' for i in range(13)],
        # Planning actions
        'uiclick_meta_action_count_PLANNING_FLAG_UPDATE',
        'uiclick_meta_action_count_PLANNING_FLAG_PLACED',
        'uiclick_meta_action_count_UNDO_PLANNING_FLAG_PLACED'
    ]

    calculate_score_extremes(df)

    sum_tool_usage(df, 'uiclick_interact_count', ui_click_columns)
    sum_tool_usage(df, 'ui_flags_interaction_count', flags_columns)

    columns_to_remove = [
        'toolused_count_WIRECUTTERS_GREEN', 'toolused_count_WIRECUTTERS_RED', 'toolused_count_WIRECUTTERS_BLUE',
        'ppe_equipped_false_count', 'Trial_Info_Subjects', 'toolused_on_grass', 'toolused_on_dirt',
        'toolused_on_minecraft:air', 'toolused_on_block_beacon_bomb', 'toolused_on_asistmod:block_beacon_bomb',
        'toolused_on_block_beacon_hazard', 'toolused_on_asistmod:block_beacon_hazard', 'toolused_on_end_bricks',
        'toolused_on_stone', 'toolused_on_sand', 'toolused_on_log', 'toolused_on_stonebrick',
        'toolused_on_asistmod:block_bomb_fire',	'toolused_on_asistmod:block_bomb_standard',
        'toolused_on_asistmod:block_bomb_chained',	'toolused_on_block_bomb_chained', 'toolused_on_block_bomb_standard',
        'objectstatechange_outcome_count_block_bomb_chained_PERTURBATION_FIRE_TRIGGER',
        'objectstatechange_outcome_count_block_bomb_fire_PERTURBATION_FIRE_TRIGGER',
        'objectstatechange_outcome_count_block_bomb_standard_PERTURBATION_FIRE_TRIGGER'
    ] + columns_with_suffix + columns_with_prefix

    # Including dynamically generated column names for uiclick and ITEMSTACK based on patterns
    columns_to_remove += [col for col in df.columns if 'uiclick_element_id_' in col or 'uiclick_meta_action_count_' in col]
    columns_to_remove += [col for col in df.columns if 'ITEMSTACK' in col]

    remove_specified_columns(df, columns_to_remove)

    # Save the modified DataFrame to a new CSV file
    df.to_csv(output_filepath, index=False)


def clean_and_save_csv(input_filepath, output_filepath):
    # Load the post-processed CSV file into a DataFrame
    df = pd.read_csv(input_filepath)

    # Filter out rows based on 'Mission_State_Change_Outcome'
    removal_conditions = [
        '',  # Blank entries
        'MISSION_STOP_SERVER_CRASHED',
        'MISSION_STOP_PLAYER_LEFT',
        'MISSION_STOP_CLIENTMAP_DISCONNECT',
        'MISSION_STOP_MINECRAFT_DISCONNECT'
    ]
    df = df[~df['Mission_State_Change_Outcome'].isin(removal_conditions)]

    # Further remove rows based on specified conditions
    df = df[df['Max_Store_Visits_flocking'] <= 600]  # Condition a
    df = df[df['team_budget_lowest'] >= -1]  # Condition b

    # Save the cleaned DataFrame to a new CSV file
    df.to_csv(output_filepath, index=False)


def post_process_trial_summaries(trial_summary_profiled_file_path,
                                 output_trial_summary_profiled_post_processed_file_path,
                                 output_trial_summary_profiled_cleaned_file_path,
                                 trial_level_team_profiles_file_path,
                                 output_trial_summary_profiled_surveys_file_path
                                 ):
    # input_filepath = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_TrialSummary_Profiled.csv'
    # output_filepath = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_TrialSummary_Profiled_PostProcessed.csv'
    process_csv(trial_summary_profiled_file_path, output_trial_summary_profiled_post_processed_file_path)

    # cleaning_input_filepath = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_TrialSummary_Profiled_PostProcessed.csv'
    # cleaning_output_filepath = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_Teams_TrialSummary_Profiles_cleaned.csv'
    clean_and_save_csv(output_trial_summary_profiled_post_processed_file_path, output_trial_summary_profiled_cleaned_file_path)

    # cleaned_df = pd.read_csv('C:\\Post-doc Work\\ASIST Study 4\\Study_4_Teams_TrialSummary_Profiles_cleaned.csv')
    cleaned_df = pd.read_csv(output_trial_summary_profiled_cleaned_file_path)
    # team_profiles_surveys_df = pd.read_csv('C:\\Post-doc Work\\ASIST Study 4\\Study_4_trialLevel_TeamProfiles.csv')
    team_profiles_surveys_df = pd.read_csv(trial_level_team_profiles_file_path)
    merged_df = pd.merge(cleaned_df, team_profiles_surveys_df, on='trial_id', how='inner')
    # merging_output_filepath = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_Teams_TrialSummary_Profiles_Surveys.csv'

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(output_trial_summary_profiled_surveys_file_path, index=False)


#####################################
# functions for splitting time series
#####################################

def split_time_series(processed_time_series_cleaned_profiled_dir_path,
                      output_player_state_items_objects_dir_path,
                      output_player_state_flocking_dir_path,
                      output_flocking_dir_path,
                      output_team_behaviors_asi_flocking_dir_path,
                      output_team_behaviors_flocking_dir_path,
                      output_team_behaviors_asi_dir_path):
    # Define the input folder
    # input_folder = 'C:\\Post-doc Work\\ASIST Study 4\\Processed_TimeSeries_CSVs_Cleaned_Profiled'

    # Secondary columns that must be retained if primary columns have data
    secondary_columns = [
        "elapsed_milliseconds_stage", "timestamp", "experiment_id", "elapsed_milliseconds_global",
        "trial_id", "timestamp_numeric", "participant_id", "estimated_elapsed_ms" , "player_teamwork_potential_score",
        "player_taskwork_potential_score",	"player_teamwork_potential_category",
        "player_taskwork_potential_category_liberal",	"player_taskwork_potential_category_conservative",
        "team_teamwork_potential_score",	"team_teamwork_potential_category",	"team_taskwork_potential_score_liberal",
        "team_taskwork_potential_category_liberal",	"team_taskwork_potential_score_conservative",
        "team_taskwork_potential_category_conservative", 'geometric_alignment_allAttributes',
        'physical_alignment_allAttributes', 'algebraic_alignment_allAttributes',
        'centroid_physical_alignment_allAttributes', 'geometric_alignment_teamworkAttributes',
        'physical_alignment_teamworkAttributes', 'algebraic_alignment_teamworkAttributes',
        'centroid_physical_alignment_teamworkAttributes', 'geometric_alignment_taskworkAttributes',
        'physical_alignment_taskworkAttributes', 'algebraic_alignment_taskworkAttributes',
        'centroid_physical_alignment_taskworkAttributes']

    # Define configurations for each set of primary columns and their output directories and suffixes
    configurations = [
        {
            # "output_folder": 'C:\\Post-doc Work\\ASIST Study 4\\Processed_TimeSeries_Split_DataSheets\\PlayerState_ItemsObjects',
            "output_folder": output_player_state_items_objects_dir_path,
            "primary_columns": [
                "playerstatechanged_ppe_equipped", "playerstatechanged_player_z", "playerstatechanged_source_z",
                "playerstatechanged_is_frozen", "player_state_x", "playerstatechanged_source_x", "itemused_target_x",
                "toolused_target_block_y", "objectstatechange_outcome", "player_state_y", "playerstatechanged_health",
                "player_state_z", "player_state_pitch", "itemused_target_y", "playerstatechanged_player_y",
                "objectstatechange_triggering_entity", "player_state_yaw", "playerstatechanged_source_y",
                "objectstatechange_id",
                "playerstatechanged_changedattributes", "sprinting", "itemused_item_name", "objectstatechange_y",
                "objectstatechange_x", "playerstatechanged_source_type", "playerstatechanged_source_id",
                "itemused_target_z",
                "objectstatechange_sequence", "itemused_item_id", "playerstatechanged_player_x", "objectstatechange_type"
            ],
            "suffix": "_PlayerState_ItemsObjects.csv"
        },

        {
            # "output_folder": 'C:\\Post-doc Work\\ASIST Study 4\\Processed_TimeSeries_Split_DataSheets\\PlayerState_Flocking',
            "output_folder": output_player_state_flocking_dir_path,
            "primary_columns": [
                "player_state_x", "player_state_y", "player_state_z", "player_state_pitch", "player_state_yaw",
                "sprinting", "flocking_alignment", "flocking_separation", "flocking_phase", "flocking_td",
                "flocking_period",
                "flocking_cohesion"
            ],
            "suffix": "_PlayerState_Flocking.csv"
        },

        {
            # "output_folder": 'C:\\Post-doc Work\\ASIST Study 4\\Processed_TimeSeries_Split_DataSheets\\Flocking',
            "output_folder": output_flocking_dir_path,
            "primary_columns": [
                "flocking_alignment", "flocking_separation", "flocking_phase", "flocking_td",
                "flocking_period",
                "flocking_cohesion"
            ],
            "suffix": "_Flocking.csv"
        },

        {
            # "output_folder": 'C:\\Post-doc Work\\ASIST Study 4\\Processed_TimeSeries_Split_DataSheets\\TeamBehaviors_ASI_Flocking',
            "output_folder": output_team_behaviors_asi_flocking_dir_path,
            "primary_columns": [
                "flocking_alignment", "flocking_separation", "flocking_phase", "flocking_td",
                "flocking_period", "flocking_cohesion",
                "objectstatechange_outcome", "toolused_tool_type", "playerstatechanged_health",
                "playerstatechanged_ppe_equipped", "playerstatechanged_is_frozen", "chat_text", "communicationchat_message",
                "communicationenvironment_message", "uiclick_meta_action", "communicationenvironment_sender_type",
                "communicationchat_environment", "interventionchat_b_content", "communicationenvironment_recipients",
                "interventionchat_content", "interventionchat_receivers", "objectstatechange_triggering_entity",
                "communicationenvironment_sender_x", "communicationchat_message_id", "teamscore",
                "interventionchat_b_receivers", "communicationchat_sender_id", "communicationenvironment_sender_y",
                "objectstatechange_id", "playerstatechanged_changedattributes", "uiclick_element_id", "sprinting",
                "itemused_item_name", "objectstatechange_y", "objectstatechange_x", "playerstatechanged_source_type",
                "playerstatechanged_source_id", "interventionresponse_agent_id", "communicationchat_source",
                "communicationenvironment_source", "interventionchat_id", "objectstatechange_sequence", "itemused_item_id",
                "interventionresponse_response_index", "playerstatechanged_player_x", "communicationenvironment_message_id",
                "objectstatechange_type", "interventionchat_b_response_options", "team_budget", "state_change_outcome",


            ],
            "suffix": "_TeamBehaviors_ASI_Flocking.csv"
        },

        {
            # "output_folder": 'C:\\Post-doc Work\\ASIST Study 4\\Processed_TimeSeries_Split_DataSheets\\TeamBehaviors_Flocking',
            "output_folder": output_team_behaviors_flocking_dir_path,
            "primary_columns": [
                "flocking_alignment", "flocking_separation", "flocking_phase", "flocking_td",
                "flocking_period", "flocking_cohesion", "participant_id", "overlap", "nonoverlap", "straightline_distance",
                "incremental_distance", "stationary_time", "ratio",
                                                        
                "objectstatechange_outcome", "toolused_tool_type", "playerstatechanged_health",
                "playerstatechanged_ppe_equipped", "playerstatechanged_is_frozen", "chat_text", "communicationchat_message",
                "communicationenvironment_message", "uiclick_meta_action", "communicationenvironment_sender_type",
                "communicationchat_environment", "communicationenvironment_recipients",
                "objectstatechange_triggering_entity",
                "communicationenvironment_sender_x", "communicationchat_message_id", "teamscore",
                "communicationchat_sender_id", "communicationenvironment_sender_y",
                "objectstatechange_id", "playerstatechanged_changedattributes", "uiclick_element_id", "sprinting",
                "itemused_item_name", "objectstatechange_y", "objectstatechange_x", "playerstatechanged_source_type",
                "playerstatechanged_source_id", "communicationchat_source",
                "communicationenvironment_source", "objectstatechange_sequence", "itemused_item_id",
                "playerstatechanged_player_x", "communicationenvironment_message_id",
                "objectstatechange_type", "team_budget", "state_change_outcome",

                "mission_stage", "transitions_to_shop", "transitions_to_field", "transitionstofield"
            ],
            "suffix": "_TeamBehaviors_Flocking.csv"
        },

        {
            # "output_folder": 'C:\\Post-doc Work\\ASIST Study 4\\Processed_TimeSeries_Split_DataSheets\\TeamBehaviors_ASI',
            "output_folder": output_team_behaviors_asi_dir_path,
            "primary_columns": [
                "objectstatechange_outcome", "toolused_tool_type", "playerstatechanged_health",
                "playerstatechanged_ppe_equipped", "playerstatechanged_is_frozen", "chat_text", "communicationchat_message",
                "communicationenvironment_message", "uiclick_meta_action", "communicationenvironment_sender_type",
                "communicationchat_environment", "interventionchat_b_content", "communicationenvironment_recipients",
                "interventionchat_content", "interventionchat_receivers", "objectstatechange_triggering_entity",
                "communicationenvironment_sender_x", "communicationchat_message_id", "teamscore",
                "interventionchat_b_receivers", "communicationchat_sender_id", "communicationenvironment_sender_y",
                "objectstatechange_id", "playerstatechanged_changedattributes", "uiclick_element_id", "sprinting",
                "itemused_item_name", "objectstatechange_y", "objectstatechange_x", "playerstatechanged_source_type",
                "playerstatechanged_source_id", "interventionresponse_agent_id", "communicationchat_source",
                "communicationenvironment_source", "interventionchat_id", "objectstatechange_sequence", "itemused_item_id",
                "interventionresponse_response_index", "playerstatechanged_player_x", "communicationenvironment_message_id",
                "objectstatechange_type", "interventionchat_b_response_options", "team_budget", "state_change_outcome",
                "teamscore", "playerscore"

            ],
            "suffix": "_TeamBehaviors_ASI.csv"
        }

    ]

    # Iterate over all CSV files in the input folder
    for file in tqdm(os.listdir(processed_time_series_cleaned_profiled_dir_path)):
        if file.endswith(".csv"):
            file_path = os.path.join(processed_time_series_cleaned_profiled_dir_path, file)
            # Read the CSV file
            df = pd.read_csv(file_path, low_memory=False)

            for config in configurations:
                output_folder, primary_columns, suffix = config['output_folder'], config['primary_columns'], config[
                    'suffix']

                # Make sure the output folder exists
                os.makedirs(output_folder, exist_ok=True)

                # Filter rows where any of the primary columns have data
                filtered_df = df.dropna(subset=primary_columns, how='all')

                # Include secondary columns explicitly
                final_columns = primary_columns + [col for col in secondary_columns if col in filtered_df.columns]
                final_df = filtered_df[final_columns]

                # Generate output file name and path
                output_file_name = file.replace('.csv', suffix)
                output_file_path = os.path.join(output_folder, output_file_name)

                # Save the filtered dataframe to a new CSV
                final_df.to_csv(output_file_path, index=False)

    # print("All files processed successfully.")


##############################################
# functions for splitting flocking time series
##############################################

def create_subfolders(base_path):
    periods = ['10', '30', '60', '180']
    for period in periods:
        folder_path = os.path.join(base_path, f'Period_{period}')
        os.makedirs(folder_path, exist_ok=True)

def split_csv_files(base_path):
    files = glob.glob(os.path.join(base_path, '*.csv'))
    periods = ['10', '30', '60', '180']

    # total_files = len(files)
    # processed_files = 0

    for file in tqdm(files, desc='  Splitting CSV files'):
        df = pd.read_csv(file, low_memory=False)
        filename = os.path.basename(file)
        for period in periods:
            # Retain all rows not related to flocking or related to the specific period
            period_df = df[(df['flocking_period'].isna()) | (df['flocking_period'] == int(period))]
            output_folder = os.path.join(base_path, f'Period_{period}')
            output_file = os.path.join(output_folder, f'{os.path.splitext(filename)[0]}_Period_{period}.csv')
            period_df.to_csv(output_file, index=False)

        # processed_files += 1
        # print(f'Status: {processed_files}/{total_files} files processed ({(processed_files / total_files) * 100:.2f}%)')


def split_flocking_time_series(team_behaviors_flocking_dir_path):
    # base_path = r'C:\Post-doc Work\ASIST Study 4\Processed_TimeSeries_Split_DataSheets\TeamBehaviors_Flocking'
    create_subfolders(team_behaviors_flocking_dir_path)
    split_csv_files(team_behaviors_flocking_dir_path)
    # print("All files have been processed.")


###################################
# functions for removing store time
###################################


def remove_store_time(df):
    """Remove rows where the mission stage is 'STORE_STAGE'."""
    # Fill forward the mission_stage values to propagate 'STORE_STAGE' and 'FIELD_STAGE' across the rows
    df['mission_stage_filled'] = df['mission_stage'].ffill()

    # Filter out rows that are in the 'STORE_STAGE'
    df_filtered = df[df['mission_stage_filled'] != 'STORE_STAGE']

    # Drop the helper column
    df_filtered = df_filtered.drop(columns=['mission_stage_filled'])

    return df_filtered


def generate_short_filename(long_filename):
    """Generate a short, unique filename using a hash."""
    hash_object = hashlib.md5(long_filename.encode())
    short_filename = hash_object.hexdigest() + '.csv'
    return short_filename


def write_store_time_removed(team_behaviors_flocking_dir_path):
    # print('jere', type(team_behaviors_flocking_dir_path))
    # Define input and output directories
    input_dir = os.path.join(team_behaviors_flocking_dir_path, "Period_10")
    # input_dir = r'C:\Post-doc Work\ASIST Study 4\Processed_TimeSeries_Split_DataSheets\TeamBehaviors_Flocking\Period_10'
    output_dir_path = os.path.join(input_dir, 'StoreTimeRemoved')

    # Create output directory if it doesn't exist
    os.makedirs(output_dir_path, exist_ok=True)

    # Process each CSV file in the input directory
    for filename in tqdm(os.listdir(input_dir)):
        if filename.endswith('.csv'):
            # Read the CSV file
            filepath = os.path.join(input_dir, filename)
            try:
                df = pd.read_csv(filepath, low_memory=False)
            except Exception as e:
                print(f"Error reading {filename}: {e}")
                continue

            # Remove store time rows
            df_filtered = remove_store_time(df)

            # Generate a short output filename
            short_filename = generate_short_filename(filename)
            output_filepath = os.path.join(output_dir_path, short_filename)

            try:
                df_filtered.to_csv(output_filepath, index=False)
            except Exception as e:
                print(f"Error saving {filename}: {e}")

    # print("Processing complete. Filtered files are saved in 'StoreTimeRemoved' folder.")
