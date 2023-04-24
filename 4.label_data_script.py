import pandas as pd
from datetime import timedelta, datetime
import numpy as np
import os
import multiprocessing

print("Reading 1 ...")
PATH = 'C:\\Users\\Shubham\\OneDrive - University of Essex\\doi_10.5061_dryad.5hqbzkh6f__v6\\merged_data_5C'

df = pd.read_csv(os.path.join(PATH, '5Cmerged_data.csv'), dtype={'id': str})
df['datetime'] = pd.to_datetime(df['datetime'].apply(lambda x: x * (10 ** 9)))


print("Reading 2 ...")
survey_path = "C:\\Users\\Shubham\\OneDrive - University of Essex\\doi_10.5061_dryad.5hqbzkh6f__v6\\SurveyResults.xlsx"

survey_df = pd.read_excel(survey_path, usecols=['ID', 'Start time', 'End time', 'date', 'Stress level'], dtype={'ID': str})
survey_df['Stress level'].replace('na', np.nan, inplace=True)
survey_df.dropna(inplace=True)

survey_df['Start datetime'] = pd.to_datetime(survey_df['date'].map(str) + ' ' + survey_df['Start time'].map(str))
survey_df['End datetime'] = pd.to_datetime(survey_df['date'].map(str) + ' ' + survey_df['End time'].map(str))
survey_df.drop(['Start time', 'End time', 'date'], axis=1, inplace=True)

print("Converting ...")
daylight = pd.to_datetime(datetime(2020, 11, 1, 0, 0, 0))


survey_df1 = survey_df[survey_df['End datetime'] <= daylight].copy()
survey_df1['Start datetime'] = survey_df1['Start datetime'].apply(lambda x: x + timedelta(hours=5))
survey_df1['End datetime'] = survey_df1['End datetime'].apply(lambda x: x + timedelta(hours=5))

survey_df2 = survey_df.loc[survey_df['End datetime'] > daylight].copy()
survey_df2['Start datetime'] = survey_df2['Start datetime'].apply(lambda x: x + timedelta(hours=6))
survey_df2['End datetime'] = survey_df2['End datetime'].apply(lambda x: x + timedelta(hours=6))

survey_df = pd.concat([survey_df1, survey_df2], ignore_index=True)

survey_df = survey_df.loc[survey_df['Stress level'] != 1.0]
survey_df.reset_index(drop=True, inplace=True)

print('Labelling ...')

if 'id' in df.columns:
    ids = df['id'].unique()
else:
    raise KeyError("'id' column not found in DataFrame 'df'.")

def parallel(id):
    new_df = pd.DataFrame(columns=['X', 'Y', 'Z', 'EDA', 'HR', 'TEMP', 'id', 'datetime', 'label'])

    sdf = df[df['id'] == id].copy()
    survey_sdf = survey_df[survey_df['ID'] == id].copy()

    for _, survey_row in survey_sdf.iterrows():
        ssdf = sdf[(sdf['datetime'] >= survey_row['Start datetime']) & (sdf['datetime'] <= survey_row['End datetime'])].copy()

        if not ssdf.empty:
            ssdf['label'] = np.repeat(survey_row['Stress level'], len(ssdf.index))
            new_df = pd.concat([new_df, ssdf], ignore_index=True)
        else:
            print(f"{survey_row['ID']} is missing label {survey_row['Stress level']} at {survey_row['Start datetime']} to {survey_row['End datetime']}")
        
    return new_df

if __name__ == '__main__':
    multiprocessing.freeze_support()

    pool = multiprocessing.Pool(len(ids))
    results = pool.map(parallel, ids)
    pool.close()
    pool.join()

    new_df = pd.concat(results, ignore_index=True)
    

    print('Saving ...')
    new_df.to_csv(os.path.join(PATH, 'label_5C_final.csv'), index=False)
    print('Done')
