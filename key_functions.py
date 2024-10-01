import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector as connector
import requests
import re
import box_update 
import time
import passwords
from bs4 import BeautifulSoup

password = passwords.password()

user = passwords.user()

def create_initial_table_sql(year):
    year_string = 'nbaseason' + str(int(year) - 1) + '_' + str(year)[2:]
    print(year_string)
    with open('initial_tables.sql') as file:
        contents = file.read()
        contents = contents.replace('database_year_name', year_string)
        connection = connector.connect(user = user, password = password)    
        cursor = connection.cursor()
        cursor.execute(contents)
        connection.close()
        return contents

def season_url(year):
    return 'https://www.basketball-reference.com/leagues/NBA_'+ year + '_games.html'

def month_url(year, month):
    return season_url(year).split('.html')[0] + '-' + month + '.html'

def get_months(year):
    response = requests.get(season_url(year))
    return[x.split('-')[1] for x in re.findall(r'/leagues/NBA_' + year + '_games-[A-Za-z]+', response.text)]

def get_season_games(year):
    season_games = pd.DataFrame()
    for month in get_months(year):        
        response = requests.get(month_url(year, month))
        month_frame = pd.read_html(response.text)[0]
        references = re.findall(r'href="/boxscores/[\w]{12}.html"', response.text)
        if 'Playoffs' in list(month_frame['Date']):
            index = month_frame[month_frame['Date'] == 'Playoffs'].index[0]
            month_frame = month_frame.iloc[:index]
            month_frame['references'] = [ref[6:-1] for ref in references][:index]
            season_games = pd.concat([season_games, month_frame])
            break
        else:
            month_frame['references'] = [ref[6:-1] for ref in references]
            season_games = pd.concat([season_games, month_frame])
    season_games.reset_index(inplace=True, drop = True)
    if 'Play-In Game' in list(season_games['Notes']):
        season_games = season_games[:season_games[season_games['Notes'] == 'Play-In Game'].index[0]]
    season_games['Date'] = pd.to_datetime(season_games['Date']).astype(str)
    season_games['Attend.'] = season_games['Attend.'].fillna(value = 0).astype(int)
    season_games['Start (ET)'] = [x[:-1] for x in season_games['Start (ET)']]
    season_games = season_games[['Home/Neutral','Visitor/Neutral','PTS.1','PTS','Date','Start (ET)','Attend.','references']]
    return season_games

def create_games_insert_query(year, season_games):
    games_values_list = []
    for x in range(len(season_games)):
        values = tuple(season_games.iloc[x])
        games_values_list.append(values)
    games_insert_query = str(games_values_list)[1:-1]
    games_insert_query = re.sub(r"np.int64\((\d+)\)", r"\1", games_insert_query)
    games_insert_query = "INSERT INTO games(Home, Away, HomeScore, AwayScore, Date, Time, Attendence, url) VALUES " + games_insert_query
    return games_insert_query

def create_teams_insert_query(year, season_games):
    teams = pd.DataFrame(season_games['Home/Neutral'].unique(), columns = ['Teams'])
    teams['City'] = [' '.join(x[:-1]) for x in teams['Teams'].str.split(' ')]
    teams['City'] = teams['City'].replace({'Portland Trail':'Portland'})
    teams['Mascot'] = [x[-1] for x in teams['Teams'].str.split(' ')]
    teams['Mascot'] = teams['Mascot'].replace({'Blazers': 'Trail Blazers'})
    team_values_list = []
    for x in range(len(teams)):
        values = tuple(teams.iloc[x])
        team_values_list.append(values)
    team_values_list = str(team_values_list)[1:-1]
    teams_insert_query = "INSERT INTO teams (Name, City, Mascot) VALUES " + team_values_list
    return teams_insert_query

def insert_games_and_teams(year):
    connection = connector.connect(user = user, password = password)    
    cursor = connection.cursor()
    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:]) 
    season_games = get_season_games(year)
    cursor.execute(create_games_insert_query(year, season_games))
    cursor.execute(create_teams_insert_query(year, season_games))
    connection.commit()
    connection.close()

def get_unique_dates(year):
    connection = connector.connect(user = user, password = password)    
    cursor = connection.cursor()    
    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:])    
    dates = pd.read_sql("SELECT GameID, Date, Home, Away, url FROM games", connection)
    unique_dates = dates['Date'].astype(str).unique()
    return unique_dates

def update_all_dates(year):
    for date in get_unique_dates(year):
        box_update.update_date(date, year)
        time.sleep(60)

def update_rosters(year):
    response = requests.get(url = 'https://www.basketball-reference.com/leagues/NBA_' + str(year) + '.html')
    htmls = set(re.findall(r'href="/teams/[\w]{3}/' + year + '.html', response.text))
    htmls = [html[7:] for html in htmls]
    year_format = str(int(year) - 1) + '-' + str(year)[2:]
    connection = connector.connect(user = user, password = password)
    cursor = connection.cursor()
    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:])
    cursor.execute("ALTER TABLE players ADD COLUMN Height_Inches INT")
    teams = pd.read_sql("SELECT TeamID, Name FROM teams", connection)
    teams_mapper = dict(zip(teams['Name'],teams['TeamID']))
    for html in htmls:
        roster_html = requests.get(url = 'https://www.basketball-reference.com/' + html)
        team_name = re.search(rf'{year_format} ([\w /]+) Roster and Stats', roster_html.text).groups()[0]
        roster = pd.read_html('https://www.basketball-reference.com/' + html)[0]
        roster = roster[['Player','No.','Pos','Ht','Wt','Birth Date']]
        roster['No.'] = roster['No.'].fillna(0)
        roster['No.'] = pd.Series([x[0] for x in roster['No.'].astype(str).str.split(',')])
        roster['TeamID'] = teams_mapper[team_name]
        roster['Height_Inches'] = [int(x[0]) * 12 + int(x[1]) for x in roster['Ht'].str.split('-')]
        roster['Birth Date'] = pd.to_datetime(roster['Birth Date']).astype(str)
        roster['Wt'] = roster['Wt'].astype(int)
        for x in range(len(roster)):
            query_string = (f"UPDATE players SET Number = {roster.iloc[x][1]}, Position = '{roster.iloc[x][2]}', " 
                    f"Height = '{roster.iloc[x][3]}', Weight = {roster.iloc[x][4]}, DOB = '{roster.iloc[x][5]}', "
                    f'Height_Inches = {roster.iloc[x][7]} WHERE Name = "{roster.iloc[x][0]}"')
            print(query_string)
            cursor.execute(query_string)
        time.sleep(10)
    cursor.execute("ALTER TABLE players ADD COLUMN Age INT")
    first_night = get_unique_dates(year)[0]
    cursor.execute(f'UPDATE players SET Age = TIMESTAMPDIFF(YEAR, DOB, "{first_night}")')
    connection.commit()
    connection.close()

def update_gamestats_wins_losses(year):
    connection = connector.connect(user = user, password = password)
    cursor = connection.cursor()
    cursor.execute("USE nbaseason" + str(int(year) - 1) + '_' + str(year)[2:])
    cursor.execute("ALTER TABLE gamestats ADD COLUMN Win_Loss VARCHAR(2)")
    cursor.execute("UPDATE gamestats SET Win_Loss = 'W' WHERE GameID IN (SELECT GameID FROM games WHERE HomeScore = GREATEST(HomeScore, AwayScore)) AND Home_Away = 'Home'")
    cursor.execute("UPDATE gamestats SET Win_Loss = 'L' WHERE GameID IN (SELECT GameID FROM games WHERE HomeScore != GREATEST(HomeScore, AwayScore)) AND Home_Away = 'Home'")
    cursor.execute("UPDATE gamestats SET Win_Loss = 'W' WHERE GameID IN (SELECT GameID FROM games WHERE AwayScore = GREATEST(HomeScore, AwayScore)) AND Home_Away = 'Away'")
    cursor.execute("UPDATE gamestats SET Win_Loss = 'L' WHERE GameID IN (SELECT GameID FROM games WHERE AwayScore != GREATEST(HomeScore, AwayScore)) AND Home_Away = 'Away'")
    connection.commit()
    connection.close()
