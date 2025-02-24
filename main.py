# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import datetime

import pandas as pd
import talib
import akshare as ak

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    concept = ak.stock_board_concept_name_em()
    print(concept)
    print(concept['板块名称'].nunique())
    print(concept['板块名称'].unique())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
