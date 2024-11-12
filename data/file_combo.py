import pandas as pd
import numpy as np
import csv
import re

# lst of column names which needs to be string
lst_str_cols = ['weight', 'height', 'medal']
# use dictionary comprehension to make dict of dtypes
dict_dtypes = {x : 'str'  for x in lst_str_cols}

# READ IN TSV DATA
orig_tsv = pd.read_csv('ORIG_all-winter-olympia.tsv', dtype=dict_dtypes, usecols=[0,1,4,5,7,10], encoding='utf-8', sep='\t')# index_col="id")
# orig_tsv = pd.read_csv('proto_tests/test.tsv', usecols=[0,1], encoding='utf-8', sep='\t')# index_col="id")
# for index, row in orig_tsv.iterrows():
#     row['noc'] = re.split('-[0-9]', str(row['noc']))[0]

# READ IN CSV DATA
orig_csv = pd.read_csv('2018+2022_Winter_Olympic_Data.csv', dtype=dict_dtypes, encoding='utf-8', sep=',')
# orig_csv = pd.read_csv('proto_tests/test.csv', dtype=dict_dtypes, encoding='utf-8', sep=',')

orig_tsv["weight"].fillna('NA', inplace = True)
orig_tsv["height"].fillna('NA', inplace = True)
# orig_tsv["medal"].fillna('NA', inplace = True)

orig_csv["weight"].fillna('NA', inplace = True)
orig_csv["height"].fillna('NA', inplace = True)
orig_csv["medal"].fillna('NA', inplace = True)

# Gets all unique athletes in 2018+22 CSV
uniqs = orig_csv.groupby('id').first().reset_index()
# print(uniqs, "\n\n")

# f = open('test_output.tsv', 'wb')  # write out in binary
# writer = pd.writer(f, dialect='excel', encoding='utf-8')
# writer = 
# f.seek(0)

# header = ["id", "name", "sex", "age", "height", "weight", "team", "noc", "year", "city", "sport", "event", "medal"]
# writer.writerow(header)


# FUNCTION TO REMOVE ALL NON-ASCII from CSV's names, TO MATCH WITH TSV FILE NAMES FOR SEARCH
# def remove_non_ascii(text):
#     return re.sub(r'[^\x00-\x7F]+', '', text)

# Returns df list of athletes that are "the same." 
# USAGE: Search TSV for CSV athl to see if CSV athl competed prior to 2014.
def loc_same_athl(name, h, w, sport, noc, df):
    # return df.loc[(df['name'] == name) & ((df['noc'] == noc) | (df['height'] == h) | (df['sport'] == sport))]
    return df.loc[((df['name'] == name) & (df['height'] == h) & (df['weight'] == w)) | ((df['name'] == name) & (df['sport'] == sport) & (df['noc'] == noc))]

    # name & height & weight    OR      name & sport & noc
    # EXCEPTIONS: people with a height/weight change AND (either a sport change OR NOC change).
        # height/weight added, NOC changed.

    # name & noc      OR      name & sport                 OR      name & height

    # there are athletes who compete in multiple sports.
    # there are some athletes who might've changed heights / weights.
    # there are some athletes for whom height AND/OR weight is missing.
    # there are some athletes who've competed for multiple NOC, e.g. https://www.olympedia.org/athletes/109630



# given name string from CSV (@LIYA UNCONVERTED OR CONVERTED??), returns the corresponding ID if it exists in TSV,
# None otherwise
def get_tsv_id(str, h, w, sport, noc, df):
    # print("\nRUNS GET_TSV_ID.")
    name = re.sub(r'[^\x00-\x7F]+', '', str)
    # print("TTTTSV WEIGHT TYPE: ", type(w))

    search_item = loc_same_athl(name, h, w, sport, noc, df)
    # search_item = df.loc[(df['name'] == name)]
    # search_item = df.loc[(df['name'] == name) & (df['noc'] == noc) & ((df['height'] == h) | (df['weight'] == w) | (df['sport'] == sport))]
    # search_item = df.loc[(df['name'] == name) & ((df['height'] == h) | (df['weight'] == w))]
    # search_item = df.loc[(df['name'] == name) & (df['height'] == h) & (df['weight'] == w)]
    # print(search_item)
    if not search_item.empty:
        # print("\nSEARCH_ITEM NONEMPTY, NEW ID IS ", search_item.iloc[0]['id'])
        return search_item.iloc[0]['id']
    else: return None


new_id_num = 135572

for i, athl in uniqs.iterrows():
    
    # get name, height, weight of unique athl from CSV
    orig_name = athl['name']
    orig_id = athl['id']
    orig_height = athl['height']
    orig_weight = athl['weight']
    orig_sport = athl['sport']
    orig_noc = athl['noc']
    # print("CSV WEIGHT TYPE: ", type(orig_weight))
    
    # store corresponding, correct ID from TSV if exists
    # print("\n\nATHLETE: ", orig_name, "\nNOC: ", orig_noc, "\nSPORT: ", orig_sport)
    corr_id = get_tsv_id(orig_name, orig_height, orig_weight, orig_sport, orig_noc, orig_tsv)
    # print("CORRESPONDING ID FOR ", orig_name, "IS ", corr_id)

    tsv_id_exists = True if not isinstance(corr_id, type(None)) else False
    # print("TSV_ID_EXISTS IS ", tsv_id_exists)

    # if new ID exists (2018+22 athl is in orig_csv), update orig_csv's 'id' col for orig 'athl'
    for index, x in orig_csv.loc[(orig_csv['id'] == orig_id)].iterrows():
                    # orig_csv.loc[(orig_csv['name'] == orig_name) & (orig_csv['noc'] == orig_noc)].iterrows():
                    # orig_csv.loc[orig_csv['name'] == orig_name & (orig_csv['noc'] == orig_noc) & ((orig_csv['height'] == orig_height) | (orig_csv['weight'] == orig_weight) | (orig_csv['sport'] == orig_sport))].iterrows():
                    # loc_same_athl(orig_name, orig_height, orig_weight, orig_sport, orig_noc, orig_csv):
                    # orig_csv.loc[orig_csv['name'] == orig_name].iterrows()
        
        orig_csv.at[index, 'id'] = corr_id if tsv_id_exists else new_id_num # str(new_id_num)


    if not tsv_id_exists: new_id_num += 1


# Sort CSV values by ID
orig_csv = orig_csv.sort_values(by='id')





################ PRINTING & WRITING


# Write new 2018+2022 CSV file with all same info but correct IDs.
# orig_csv.to_csv('RIGHT-ID_FIXUPS/OR_right-IDs_2018+2022.csv', index=False)
# orig_csv.to_csv('RIGHT-ID_FIXUPS/AND_right-IDs_2018+2022.csv', index=False)
# orig_csv.to_csv('RIGHT-ID_FIXUPS/BASE_right-IDs_2018+2022.csv', index=False)
# orig_csv.to_csv('RIGHT-ID_FIXUPS/ORORsprt_right-IDs_2018+2022.csv', index=False)
# orig_csv.to_csv('TEMP_right-IDs_2018+2022.csv', index=False)

# Append 2018+2022 data to existing TSV
# with open('RIGHT-ID_FIXUPS/right-IDs_2018+2022.csv', 'r') as csvin, open('FINAL_TSV.tsv', 'a', newline='') as tsvout:
#     csvin = csv.reader(csvin)
#     tsvout = csv.writer(tsvout, delimiter='\t')

#     headings = next(csvin)

#     for row in csvin:
#         tsvout.writerow(row)


# Convert 2018+2022 CSV data to TSV data
# with open('RIGHT-ID_FIXUPS/right-IDs_2018+2022.csv', 'r') as csvin, open('RIGHT-ID_FIXUPS/right-IDs_2018+2022.tsv', 'w', newline='') as tsvout:
#     csvin = csv.reader(csvin)
#     tsvout = csv.writer(tsvout, delimiter='\t')

#     # headings = next(csvin)

#     for row in csvin:
#         tsvout.writerow(row)








########## PSEUDOCODE
# read in all_olympia_data TSV (as csv? as tsv?)
# read in 2018+2022 CSV data (way to get all unique IDs??)   < 1st 2 cols only?? and only unique??

# @LIYA NEED TO NOW FIGURE OUT HOW TO INDEX INTO 'name' COL OF CSV PANDAS DATAFRAME
# + FIGURE OUT HOW TO SEARCH WITHIN COL OF A DATAFRAME IN PANDAS??
# @LIYA NEED TO FIGURE OUT CURRENTLY-HIGHEST ATHL ID IN TSV FILE.
    # create a counter for next-highest ID to add, if athl is not already in TSV file.

# for each unique ID in "id" column from CSV,
    
    # convert athl name with remove_non_ascii.
    # store both orig athl string (from 2018+2022) and new, remove_non_ascii'd athlete string
    
    # search for that converted athl's name in TSV file.            < NEED TO TEST WHETHER SPECIAL CHARACTERS SEARCHABLE.
    # if match is found,
        # update ALL athlete IDs for that (orig) athlete name in 2018+2022 CSV.
    # else
        # 