import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import mysql.connector as connector
import re
import passwords

password = passwords.password()

user = passwords.user()

def get_dates_urls(Date, connection):
    dates_urls = pd.read_sql("SELECT GameID, Date, Home, Away, url FROM games WHERE Date = '{}'".format(Date), connection)
    dates_urls['url'] = 'https://www.basketball-reference.com' + dates_urls['url']
    return dates_urls


def update_box(box, number):
    box.columns = [x[1] for x in box.columns]
    box = box[~box['FG'].str.contains(r'\D')]
    box = box[box['MP'].str.contains(":")]                          
    box['MP'] = [x.split(':') for x in box['MP']]
    box['+/-'] = [0 if x != x else 0 if x[0] == '0' else int(x[1:]) if x[0] == '+' else -int(x[1:]) for x in box['+/-']]
    box['MP'] = [int(x[0]) + round(int(x[1])/60, 2) for x in box['MP']]
    box['MP'] = box['MP'].round(2)
    for x in ['FG', 'FGA', '3P', '3PA', 'FT', 'FTA', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']:
        box[x] = box[x].astype(int)
        box['Starter'] = [1] * 5 + [0] * (len(box)-5)
    return box


def update_date(Date, year):
    connection = connector.connect(user = user, password = password)

    cursor = connection.cursor()

    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:])

    dates_urls = get_dates_urls(Date, connection)

    teams = pd.read_sql("SELECT TeamID, Name FROM teams", connection)

    teams_mapper = dict(zip(teams['Name'],teams['TeamID']))

    players_list = pd.read_sql("SELECT PlayerID, TeamID, Name FROM players", connection)
    print(players_list)

    name_plus_team = list(players_list['Name'] + players_list['TeamID'].astype(str))

    player_mapper = dict(zip(name_plus_team, players_list['PlayerID'].astype(str)))

    #urls = 'https://www.basketball-reference.com' + dates_urls[dates_urls['Date'] == dates_urls['Date'].unique()[0]]['url']

    for x in range(len(dates_urls)):
        box_scores = pd.read_html(dates_urls['url'][x])           
        away_box = box_scores[0]
        home_box = box_scores[len(box_scores)//2]
        home_box = update_box(home_box, x)
        away_box = update_box(away_box, x)
        home_box['Home/Away'] = 'Home'
        away_box['Home/Away'] = 'Away'
        home_box['TeamID'] = teams_mapper[dates_urls['Home'][x]]       
        away_box['TeamID'] = teams_mapper[dates_urls['Away'][x]]  
        box_score = pd.concat([home_box, away_box])
        for player, team_ID in list(zip(box_score['Starters'], box_score['TeamID'].astype(str))):
            if player + team_ID in name_plus_team:
                pass
            else:
                print('INSERT INTO players (TeamID, Name) VALUES (' + team_ID + ', "' + player + '")')
                cursor.execute('INSERT INTO players (TeamID, Name) VALUES (' + team_ID + ', "' + player + '")')
                players_list = pd.read_sql("SELECT PlayerID, TeamID, Name FROM players", connection)
                name_plus_team = list(players_list['Name'] + players_list['TeamID'].astype(str))
                player_mapper = dict(zip(name_plus_team, players_list['PlayerID'].astype(str)))
        box_score['PlayerID'] = pd.Series(box_score['Starters'] + box_score['TeamID'].astype(str)).map(player_mapper).astype(int)
        box_score['GameID'] = dates_urls['GameID'][x]
        box_score = box_score[['GameID','TeamID','PlayerID','Starters','Starter', 'MP', 'FG', 'FGA', '3P', '3PA', 'FT', 'FTA',
                'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS',
            '+/-','Home/Away']]
        input_list = []
        for x in range(len(box_score)):
            input_list.append(tuple(box_score.iloc[x]))
        input_list = str(input_list)[1:-1]
        insert_query = """INSERT INTO gamestats (GameID, TeamID, PlayerID, Player, Starter, MP, FG, FGA, 3FG, 3FGA, \
        FT, FTA, ORB, DRB, TRB, AST, STL, BLK, TOV, \
        PF, PTS, Plus_Minus, Home_Away) VALUES {}""".format(input_list)
        insert_query = re.sub(r"np.int64\(([-]?\d+)\)", r"\1", insert_query)
        insert_query = re.sub(r"np.float64\((\d+\.\d+)\)", r"\1", insert_query)
        print(insert_query)
        cursor.execute(insert_query)
    connection.commit()
    connection.close()