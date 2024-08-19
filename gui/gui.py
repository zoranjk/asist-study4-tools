''' module for GUI '''
from processing import download, extract, dedup, etl
from .console_capture import RedirectStdout

import json
import sys
import os
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
from pathlib import Path

# load config
with open("config/config.json") as config_file:
    config = json.load(config_file)
dir_paths = config["dir_path"]
file_paths = config["file_path"]

# GUI functions
def select_dir_path(data_dir_text):
    dir_path = filedialog.askdirectory(initialdir='.',
                                       title='Select a directory location')
    if dir_path:
        data_dir_text.set(dir_path)

def dl_dataset(dl_dir:Path):
    download.download_dataverse_dataset(config['dataset']['persistent_id'], dl_dir)

def is_valid_path(text:tk.StringVar) -> bool:
    ''' given a tk.StringVar converts it to a path and returns false if empty or invalid '''
    if text.get() == '':
        messagebox.showerror("Missing directory", "Please select a directory location for the download and try again.")
        return False
    elif not Path(text.get()).is_dir():
        messagebox.showerror("Invalid directory", "Please select a valid directory location for the download and try again.")
        return False
    return True

def strvar_to_path(text:tk.StringVar) -> Path:
    return Path(text.get())

def confirm_dl(dl_dir_text:tk.StringVar):
    confirmed = messagebox.askyesnocancel("Confirm download", "Are you sure you want to download the dataset? You may overwrite an existing download.")
    if confirmed and is_valid_path(dl_dir_text):
        dl_dataset(strvar_to_path(dl_dir_text))

def process_metadata(dl_dir_text, data_dir_text):
    dl_dir_path = strvar_to_path(dl_dir_text)
    data_dir_path = strvar_to_path(data_dir_text)
    metadata_dir_path = os.path.join(data_dir_path, dir_paths["metadata"])
    metadata_unique_dir_path = os.path.join(data_dir_path, dir_paths["metadata_unique"])

    extract.extract_metadata(dl_dir_path, metadata_dir_path)
    dedup.save_unique_files(metadata_dir_path, metadata_unique_dir_path)

def process_unique_message_subtypes():
    pass

def process_intervention_measures():
    pass

def gui():
    ''' main function that runs the GUI '''
    root = tk.Tk()
    root.title('ASIST Study 4 Analysis Tools')
    
    # dataset download location selection
    dl_frame = tk.Frame(root)
    dl_frame.pack(padx=10, pady=10)
    dl_dir_text = tk.StringVar()
    dl_loc_label = tk.Label(dl_frame, text='Dataset download location:')
    dl_loc_label.pack(side=tk.LEFT, padx=10)
    dl_loc_entry = tk.Entry(dl_frame, textvariable=dl_dir_text, width=50)
    dl_loc_entry.pack(side=tk.LEFT, padx=10)
    dl_loc_button = tk.Button(dl_frame,
                              text="Select a directory location",
                              command=lambda: select_dir_path(dl_dir_text))
    dl_loc_button.pack(side=tk.LEFT)

    
    # download dataset button
    dl_button = tk.Button(dl_frame,
                          text="Download dataset",
                          command=lambda: confirm_dl(dl_dir_text))
    dl_button.pack(side=tk.RIGHT, padx=10)


    # analysis directory selection
    an_frame = tk.Frame(root)
    an_frame.pack(padx=10, pady=10)
    data_dir_text = tk.StringVar()
    an_label = tk.Label(an_frame, text='Analysis files output directory:')
    an_label.pack(side=tk.LEFT, padx=10)
    an_entry = tk.Entry(an_frame, textvariable=data_dir_text, width=50)
    an_entry.pack(side=tk.LEFT, padx=10)
    an_button = tk.Button(an_frame,
                          text="Select a directory location",
                          command=lambda: select_dir_path(data_dir_text))
    an_button.pack(side=tk.LEFT)


    # metadata
    metadata_frame = tk.Frame(root)
    metadata_frame.pack(padx=10, pady=10)
    metadata_button = tk.Button(metadata_frame,
                                text="Process metadata files",
                                command=lambda: process_metadata(dl_dir_text,
                                                                 data_dir_text))
    metadata_button.pack(side=tk.LEFT)
    



    # # TODO: test remove this
    # button = tk.Button(root, text="Print to Console", command=lambda: print("al;skdfjal;skdf"))
    # button.pack(pady=10)


    # exit button
    exit_button = tk.Button(root, text='Exit', width=10, command=root.destroy)
    exit_button.pack(pady=10)

    root.mainloop()