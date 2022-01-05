import queue
import pandas as pd
import argparse

import riotwatcher as rw

REGIONS_ROUTING = {
    "euw1": "europe",
    "na1": "americas",
    "kr": "asia",
    "jp1": "asia",
    "tr1": "europe",
    "ru": "europe",
    "eun1": "europe",
    "la1": "americas",
    "la2": "americas",
    "oc1": "americas",
    "br1": "americas",
}

class DataMiner():
    def __init__(self,api_key,region='euw1'):
        self.api_key = api_key
        self.watcher = rw.LolWatcher(api_key)
        self.region = region
        self.region_routed = REGIONS_ROUTING[region]

        self.matches_data = []
        self.users_data = []

    def save(self):
        df_users = pd.DataFrame(self.users_data)
        df_matches = pd.DataFrame(self.matches_data)
        df_users.to_csv(f'summoners_{self.region}.csv')
        df_matches.to_csv(f'matches_{self.region}.csv')
        print('Saved!')

    def get_user_matches(self,user_puuid,user_id,n=50):
        self.get_user_data(user_puuid,user_id)
        matches = []
        idx = 0
        step = min(100,n)
        last_length = 1
        while last_length and idx < n:
            new_matches = self.watcher.match.matchlist_by_puuid(
                region=self.region_routed,
                queue=420,
                puuid=user_puuid,
                start=idx,
                count=step
            )
            matches += new_matches
            last_length = len(new_matches)
            idx += last_length
        return list(dict.fromkeys(matches))
        
    def get_user_data(self,user_puuid,user_id):
        champion_masteries = self.watcher.champion_mastery.by_summoner(
            region=self.region,
            encrypted_summoner_id=user_id
        )
        summoner_league = self.watcher.league.by_summoner(
            region=self.region,
            encrypted_summoner_id=user_id
        )
        user_data = {
            'puuid': user_puuid,
            'id': user_id,
            'tier': None,
            'rank': None,
            'lp': None,
            'wins': None,
            'losses': None,
        }
        # Get ranked soloQ rank
        for queue in summoner_league:
            if queue['queueType'] == "RANKED_SOLO_5x5":
                user_data['tier'] = queue['tier']
                user_data['rank'] = queue['rank']
                user_data['lp'] = queue['leaguePoints']
                user_data['wins'] = queue['wins']
                user_data['losses'] = queue['losses']
        # Get champion mastery information
        for champion in champion_masteries:
            user_data[champion['championId']] = champion['championPoints']
        
        self.users_data.append(user_data)

    def process_matchdto(self,matchdto):
        data = {}
        matchinfo = matchdto['info']
        data['gameId'] = matchinfo['gameId']

        # Get winner team id
        for team in matchinfo['teams']:
            if team['win']:
                data['winner'] = team['teamId']

        # Get participants data
        for i,participant in enumerate(matchinfo['participants']):
            data[f'summoner_{i}_puuid'] = participant['puuid']
            data[f'summoner_{i}_summonerId'] = participant['summonerId']
            data[f'summoner_{i}_championId'] = participant['championId']
            data[f'summoner_{i}_summonerLevel'] = participant['summonerLevel']
            data[f'summoner_{i}_teamPosition'] = participant['teamPosition']
            data[f'summoner_{i}_summoner1Id'] = participant['summoner1Id']
            data[f'summoner_{i}_summoner2Id'] = participant['summoner2Id']
            data[f'summoner_{i}_primaryStyle'] = participant['perks']['styles'][0]['style']
            data[f'summoner_{i}_subStyle'] = participant['perks']['styles'][1]['style']
            
        # Add ban datas
        for team in matchinfo['teams']:
            teamId = team['teamId']
            for i,ban in enumerate(team['bans']):
                data[f'ban_{teamId}_{i}'] = ban['championId']

        self.matches_data.append(data)

    def mine(self,init_user):
        # Users/Matches to be processed
        user_queue = queue.Queue()
        match_queue = queue.Queue()

        # Users/Matches already seen
        users_seen = {}
        matches_seen = {}

        # Put initial user into queue
        init_user_sdto = self.watcher.summoner.by_name(self.region,init_user)
        user_queue.put((init_user_sdto['puuid'],init_user_sdto['id']))

        # Mine data
        counter = 0
        while (not user_queue.empty()) and (user_queue.qsize() < 10000): # 10000 = it should take 11 hours to get remaining user data
            # Get new user matchlist
            user_puuid, user_id = user_queue.get()
            try:
                matches = self.get_user_matches(user_puuid,user_id)
            except Exception as e:
                print(e)
                user_queue.put((user_puuid, user_id))
                continue
            # Put unseen matches
            for match in matches:
                if match not in matches_seen:
                    match_queue.put(match)
                    matches_seen[match] = True
            # Process matches
            while not match_queue.empty():
                # Get new match data
                match_id = match_queue.get()
                try:
                    matchdto = self.watcher.match.by_id(
                        region = self.region_routed,
                        match_id = match_id
                    )
                except Exception as e:
                    print(e)
                    match_queue.put(match_id)
                    continue
                self.process_matchdto(matchdto)
                # Add new users 
                for new_user in matchdto['info']['participants']:
                    puuid = new_user['puuid']
                    id = new_user['summonerId']
                    summ = (puuid,id)
                    if summ not in users_seen:
                        user_queue.put(summ)
                        users_seen[summ] = True
                counter += 1

                # Save every 100 iterations
                if (counter % 100) == 0:
                    print(f'Matches remaining: {match_queue.qsize()}, summoners remaining: {user_queue.qsize()}')
                    self.save()

        self.save()
        # Get remaining user data
        while not user_queue.empty():
            user_puuid, user_id = user_queue.get()
            try:
                self.get_user_data(user_puuid,user_id)
            except Exception as e:
                print(e)
                user_queue.put((user_puuid, user_id))
                continue
            counter += 1
            if (counter % 100) == 0:
                    print(f'Matches remaining: {match_queue.qsize()}, summoners remaining: {user_queue.qsize()}')
                    self.save()
        self.save()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='League API matches/summoner data mining')
    parser.add_argument(
        '--init',
        help='Initial summoner to start with',
        default='Dzukill'
    )
    parser.add_argument(
        '--key',
        help='Riot API key',
        default='RGAPI-dd631df5-30c4-40f9-b298-cafb92af6e88'
    )
    parser.add_argument(
        '--region',
        help='Region',
        default='euw1'
    )
    args = parser.parse_args()

    miner = DataMiner(args.key,args.region)
    miner.mine(args.init)
    