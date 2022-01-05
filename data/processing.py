from re import match
import numpy as np
import pandas as pd
from tqdm import tqdm

def process_filepair(suffix):
    eps = 1e-5
    # Process a pair of files matches_{suffix}.csv, summoners_{suffix}.csv
    matches_file = f'matches_{suffix}.csv'
    summoners_file = f'summoners_{suffix}.csv'
    
    matches_df = pd.read_csv(matches_file, index_col='gameId')
    summoners_df = pd.read_csv(summoners_file, index_col='puuid')

    matches_df = matches_df[~matches_df.index.duplicated(keep='first')]
    summoners_df = summoners_df[~summoners_df.index.duplicated(keep='first')]

    for summoner in tqdm(range(10)):
        # Summoner tier 
        tier_column = summoners_df[['tier']].rename({'tier':f'summoner_{summoner}_tier'}, axis='columns')
        matches_df = matches_df.join(tier_column,on=f'summoner_{summoner}_puuid')
        # Summoner rank 
        rank_column = summoners_df[['rank']].rename({'rank':f'summoner_{summoner}_rank'}, axis='columns')
        matches_df = matches_df.join(rank_column,on=f'summoner_{summoner}_puuid')
        # Summoner lp 
        lp_column = summoners_df[['lp']].rename({'lp':f'summoner_{summoner}_lp'}, axis='columns')
        matches_df = matches_df.join(lp_column,on=f'summoner_{summoner}_puuid')
        # Winrate
        wr_column = summoners_df[['wins','losses']]
        wr_column['wr'] = wr_column['wins']/(wr_column['wins']+wr_column['losses']+eps)
        wr_column['nb'] = wr_column['wins']+wr_column['losses']
        wr_column = wr_column[['wr','nb']].rename({
            'wr':f'summoner_{summoner}_wr',
            'nb':f'summoner_{summoner}_nb'
        }, axis='columns')
        matches_df = matches_df.join(wr_column,on=f'summoner_{summoner}_puuid')

        # Summoner / champion mastery lp
        matches_df[f'summoner_{summoner}_champion_mastery'] = matches_df.apply(
            lambda row: summoners_df.at[row[f'summoner_{summoner}_puuid'], str(row[f'summoner_{summoner}_championId'])],
            axis=1
        )

    return matches_df

def create_dataset(suffix_list,name='raw_dataset.csv'):
    # suffix_list : list of suffixes of all file pairs, like euw1 na1 kr etc..
    df_list = []
    for suffix in suffix_list:
        df_list.append(process_filepair(suffix))
    dataset = pd.concat(df_list)
    dataset.to_csv(name)
    print(dataset.shape)

create_dataset(['euw1','euw1_2','na1','kr'])