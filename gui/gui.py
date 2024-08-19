''' module for GUI '''
from processing import download

import json
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path

# load config
with open("config/config.json") as config_file:
    config = json.load(config_file)

# GUI functions
def select_dir_path(data_dir_text):
    dir_path = filedialog.askdirectory(initialdir='.',
                                       title='Select a directory location')
    if dir_path:
        data_dir_text.set(dir_path)

def dl_dataset(dl_dir_text):
    download.download_dataverse_dataset(config['dataset']['persistent_id'], Path(dl_dir_text.get()))

def confirm_dl(dl_dir_text):
    confirmed = messagebox.askyesnocancel("Confirm download", "Are you sure you want to download the dataset? You may overwrite an existing download.")
    if dl_dir_text.get() == '':
        messagebox.showerror("Missing directory", "Please select a directory location for the download and try again.")
        return
    if not Path(dl_dir_text.get()).is_dir():
        messagebox.showerror("Invalid directory", "Please select a valid directory location for the download and try again.")
    elif confirmed:
        dl_dataset(dl_dir_text)


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
    dl_loc_button = tk.Button(dl_frame, text="Select a directory location", command=lambda: select_dir_path(dl_dir_text))
    dl_loc_button.pack(side=tk.LEFT)

    
    # download dataset button
    dl_button = tk.Button(dl_frame, text="Download dataset", command=lambda: confirm_dl(dl_dir_text))
    dl_button.pack(side=tk.RIGHT, padx=10)


    # analysis directory selection
    an_frame = tk.Frame(root)
    an_frame.pack(padx=10, pady=10)
    an_dir_text = tk.StringVar()
    an_label = tk.Label(an_frame, text='Analysis files output directory:')
    an_label.pack(side=tk.LEFT, padx=10)
    an_entry = tk.Entry(an_frame, textvariable=an_dir_text, width=50)
    an_entry.pack(side=tk.LEFT, padx=10)
    an_button = tk.Button(an_frame, text="Select a directory location", command=lambda: select_dir_path(an_dir_text))
    an_button.pack(side=tk.LEFT)

    # exit button
    exit_button = tk.Button(root, text='Exit', width=10, command=root.destroy)
    exit_button.pack(pady=10)

    root.mainloop()