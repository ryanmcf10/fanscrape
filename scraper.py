from functools import reduce

import requests
import pandas as pd
from urllib3.contrib import pyopenssl

class Scraper:
    session = None

    def __init__(self, *args, **kwargs):
        pyopenssl.inject_into_urllib3()

        self.session = requests.Session()

    def get_league_events(self, league_id):
        url = f'https://sportsbook.fanduel.com/cache/psmg/UK/{league_id}.json' 
        
        req = self.session.get(url)

        return req


    def get_event(self, event_id):
        url = f'https://sportsbook.fanduel.com/cache/psevent/UK/1/false/{event_id}.json'

        req = self.session.get(url)

        return req

    def parse_data(self, jsonData):
        results_df = pd.DataFrame()
        for alpha in jsonData['events']:
            print ('Gathering %s data: %s @ %s' %(alpha['sportname'],alpha['participantname_away'],alpha['participantname_home']))
            alpha_df = pd.json_normalize(alpha).drop('markets',axis=1)
            for beta in alpha['markets']:
                beta_df = pd.json_normalize(beta).drop('selections',axis=1)
                beta_df.columns = [str(col) + '.markets' for col in beta_df.columns]
                for theta in beta['selections']:
                    theta_df = pd.json_normalize(theta)
                    theta_df.columns = [str(col) + '.selections' for col in theta_df.columns]

                    temp_df = reduce(lambda left,right: pd.merge(left,right, left_index=True, right_index=True), [alpha_df, beta_df, theta_df])
                    results_df = results_df.append(temp_df, sort=True).reset_index(drop=True)

        return results_df
