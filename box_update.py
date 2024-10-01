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
    box = box[box['MP'].str.contains(":")]                          #NEEDS UPDATE FOR ITERATION
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
        box_scores = pd.read_html(dates_urls['url'][x])           # NEEDS UPDATE FOR ITERATION
        away_box = box_scores[0]
        home_box = box_scores[len(box_scores)//2]
        home_box = update_box(home_box, x)
        away_box = update_box(away_box, x)
        home_box['Home/Away'] = 'Home'
        away_box['Home/Away'] = 'Away'
        home_box['TeamID'] = teams_mapper[dates_urls['Home'][x]]       #NEEDS UPDATE FOR ITERATION
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


def update_players_table(year):
    connection = connector.connect(user = user, password = password)

    cursor = connection.cursor()

    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:])

    cursor.execute("ALTER TABLE players ADD COLUMN Height_Inches INT")

    connection.commit()
    
    connection.close()

def update_gamestats_wins_losses(year):
    connection = connector.connect(user = user, password = password)

    cursor = connection.cursor()

    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:])

    cursor.execute("ALTER TABLE gamestats ADD COLUMN Win_Loss INT")

    connection.commit()
    
    connection.close()

def update_rosters(season_url, year):
    response = requests.get(url = season_url)
    htmls = set(re.findall(r'href="/teams/[\w]{3}/2024.html', response.text))
    htmls = [html[7:] for html in htmls]
    connection = connector.connect(user = user, password = password)
    cursor = connection.cursor()
    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:])
    teams = pd.read_sql("SELECT TeamID, Name FROM teams", connection)
    teams_mapper = dict(zip(teams['Name'],teams['TeamID']))
    for html in htmls:
        roster_html = requests.get(url = 'https://www.basketball-reference.com/' + html)
        team_name = re.search(r'2023-24 ([\w ]+) Roster and Stats', roster_html.text).groups()[0]
        roster = pd.read_html(roster_html.text)[0]
        roster = roster[['Player','No.','Pos','Ht','Wt','Birth Date']]
        roster['No.'] = pd.Series([x[0] for x in roster['No.'].astype(str).str.split(',')])
        roster['TeamID'] = teams_mapper[team_name]
        roster['Height_Inches'] = [int(x[0]) * 12 + int(x[1]) for x in roster['Ht'].str.split('-')]
        roster['Birth Date'] = pd.to_datetime(roster['Birth Date']).astype(str)
        roster['Wt'] = roster['Wt'].astype(int)
        for x in range(len(roster)):
            query_string = (f"UPDATE Players SET Number = {roster.iloc[x][1]}, Position = '{roster.iloc[x][2]}', " 
                    f"Height = '{roster.iloc[x][3]}', Weight = {roster.iloc[x][4]}, DOB = '{roster.iloc[x][5]}', "
                    f'Height_Inches = {roster.iloc[x][7]} WHERE Name = "{roster.iloc[x][0]}"')
            cursor.execute(query_string)
    connection.commit()
