''' module for GUI '''

import tkinter as tk
from tkinter import filedialog

def select_dir_path(text):
    dir_path = filedialog.askdirectory(initialdir='.',
                                       title='Select a directory location')
    if dir_path:
        text.set(dir_path)

if __name__ == "__main__":
    root = tk.Tk()
    root.title('ASIST Study 4 Analysis Tools')


    # data dir selection
    frame = tk.Frame(root)
    frame.pack(padx=10, pady=10)

    data_dir_text = tk.StringVar()

    label1 = tk.Label(frame, text='Data output directory:')
    label1.pack(side=tk.LEFT, padx=10)

    entry = tk.Entry(frame, textvariable=data_dir_text, width=50)
    entry.pack(side=tk.LEFT, padx=10)

    button = tk.Button(frame, text="Select a directory location", command=lambda: select_dir_path(data_dir_text))
    button.pack(side=tk.LEFT)


    # exit button
    exit_button = tk.Button(root, text='Exit', width=10, command=root.destroy)
    exit_button.pack(pady=10)

    root.mainloop()