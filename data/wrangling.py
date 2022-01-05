'''
Transforms the raw_dataset.csv into dataset.csv, such that it can be easily fed into 
a Deep Learning/Machine Learning training pipeline
'''
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler,LabelBinarizer,LabelEncoder
from tqdm import tqdm

df = pd.read_csv('raw_dataset.csv', index_col='gameId')

# List features to be kept
categorical_features = []
continuous_features = []
for summoner in range(10):
    # Categorical
    categorical_features.append(f'summoner_{summoner}_championId')
    categorical_features.append(f'summoner_{summoner}_teamPosition')
    categorical_features.append(f'summoner_{summoner}_summoner1Id')
    categorical_features.append(f'summoner_{summoner}_summoner2Id')
    categorical_features.append(f'summoner_{summoner}_primaryStyle')
    categorical_features.append(f'summoner_{summoner}_subStyle')
    # Continuous
    #continuous_features.append(f'summoner_{summoner}_summonerLevel')
    continuous_features.append(f'summoner_{summoner}_lp')
    continuous_features.append(f'summoner_{summoner}_champion_mastery')
    continuous_features.append(f'summoner_{summoner}_wr')
    # continuous_features.append(f'summoner_{summoner}_nb')

# Only keep necessary features
all_features = ['winner'] + categorical_features + continuous_features
df = df[all_features]

feature_groups = {
    # 'team_1_level': [f'summoner_{summoner}_summonerLevel' for summoner in range(5)],
    'team_1_lp': [f'summoner_{summoner}_lp' for summoner in range(5)],
    'team_1_mastery': [f'summoner_{summoner}_champion_mastery' for summoner in range(5)],
    'team_1_wr': [f'summoner_{summoner}_wr' for summoner in range(5)],
    # 'team_1_nb': [f'summoner_{summoner}_nb' for summoner in range(5)],

    # 'team_2_level': [f'summoner_{summoner}_summonerLevel' for summoner in range(5,10)],
    'team_2_lp': [f'summoner_{summoner}_lp' for summoner in range(5,10)],
    'team_2_mastery': [f'summoner_{summoner}_champion_mastery' for summoner in range(5,10)],
    'team_2_wr': [f'summoner_{summoner}_wr' for summoner in range(5,10)],
    # 'team_2_nb': [f'summoner_{summoner}_nb' for summoner in range(5,10)]
}

for key, value in feature_groups.items():
    df[f'{key}_mean'] = df[value].mean(axis=1)
    df[f'{key}_std'] = df[value].std(axis=1)
    df[f'{key}_median'] = df[value].median(axis=1)
    df[f'{key}_skew'] = df[value].skew(axis=1)
    df[f'{key}_kurtosis'] = df[value].kurtosis(axis=1)
    df[f'{key}_variance'] = df[value].var(axis=1)

'''
# One hot encode categorical data
for cat in tqdm(categorical_features):
    df = pd.get_dummies(df,cat,columns=[cat])

# Normalize continuous data
scaler = StandardScaler()
df[continuous_features] = scaler.fit_transform(df[continuous_features])
'''
"""
le = LabelEncoder()
le.fit(df[categorical_features].values.ravel())
for cat in categorical_features:
    df[cat] = le.transform(df[cat])
print(le.classes_)
"""
print(df['team_1_wr_mean'].iloc[0])
print(df['team_1_wr_median'].iloc[0])
print(df['team_1_wr_std'].iloc[0])
print(df['team_1_wr_skew'].iloc[0])
print(df[feature_groups['team_1_wr']].iloc[0])
# Binarize label
lb = LabelBinarizer()
df[['winner']] = lb.fit_transform(df[['winner']])

# Save
df.to_csv('dataset.csv')
print(df.shape)