import pandas as pd
import numpy as np
import requests
import mysql.connector as connector
import passwords

password = passwords.password()
user = passwords.user()

def where_clause(words, where_column, select_column, table):
    clause = ''
    if type(words) == str:
        clause = f"{where_column} IN (SELECT {where_column} FROM {table} WHERE {select_column} = '{words}')"
    elif type(words) == list:
        words_join = "', '".join(words)
        clause = f"{where_column} IN (SELECT {where_column} FROM {table} WHERE {select_column} IN ('{words_join}'))"
    return clause

def condition_where(condition, where_column, condition_column, table, date = False):
    clause = ''
    if condition != None:
        if date == False:
            clause = f'{where_column} IN (SELECT {where_column} FROM {table} WHERE {condition_column} {condition})'
        else:
            clause = f'{where_column} IN (SELECT {where_column} FROM {table} WHERE {condition_column} {condition}")'
    return clause

where_clause('Boston Celtics','GameID','Home','games')

def splits(type_stats = 'per_game', teams = None, players = None, positions = None, opponents = None, min_weight = None, max_weight = None, min_age = None, min_height = None, max_height = None,

            max_age = None, start_date = None, end_date= None, Home = None, win_loss = None, by_team = 'no', include = 'no'):
    team_clause = where_clause(teams, 'TeamID','Name','teams')
    player_clause = where_clause(players, 'PlayerID','Name', 'players')
    positions_clause = where_clause(positions, 'PlayerID','Position','players')
    opponents_clause = ''
    if opponents != None:
        opponents_clause = '((' + where_clause(opponents, 'GameID','Home','games') + 'AND Home_Away = "Away") OR (' + where_clause(opponents, 'GameID','Away','games') + 'AND Home_Away = "Home"))'
    min_weight_clause=''
    if min_weight != None:
        min_weight_clause = condition_where('>' + str(min_weight), 'PlayerID', 'Weight','players')
    max_weight_clause=''
    if max_weight != None:
        max_weight_clause = condition_where('>' + str(max_weight), 'PlayerID', 'Weight','players')
    min_height_clause=''
    if min_height != None:
        min_height_clause = condition_where('>' + str(min_height), 'PlayerID', 'Height_Inches','players')
    max_height_clause=''
    if max_height != None:
        max_height_clause = condition_where('>' + str(max_height), 'PlayerID', 'Height_Inches','players')
    min_age_clause = ''
    if min_age != None:
        min_age_clause = condition_where('>=' + str(min_age), 'PlayerID','Age','players')
    max_age_clause = ''
    if max_age != None:
        max_age_clause = condition_where('<=' + str(max_age), 'PlayerID','Age','players')
    start_date_clause = ''
    if start_date != None:
        start_date_clause = condition_where('>"' +str(start_date), 'GameID','Date','Games', date = True)
    end_date_clause = ''
    if end_date != None:
        end_date_clause = condition_where('>"' +str(end_date), 'GameID','Date','Games', date = True)
    home_away_clause = ''
    if Home == 'Home':
        home_away_clause = "Home_Away = 'Home'"
    if Home == 'Away':
        home_away_clause = "Home_Away = 'Away'"
    by_team_clause = ''
    if by_team == 'yes':
        by_team_clause = ', TeamID'
    by_win_loss_clause = ''
    if win_loss == 'yes':
        by_win_loss_clause = ', Win_Loss'

        
    present_clauses = [x for x in [team_clause, player_clause, positions_clause, opponents_clause, min_weight_clause, 
                                   max_weight_clause, min_height_clause, max_height_clause, min_age_clause, max_age_clause, start_date_clause, end_date_clause, home_away_clause] if x!= '']

    where_clauses = ''

    include_clauses = ['','']
    if min_weight != None or max_weight != None:
        include_clauses[0] = include_clauses[0] + ', players.Weight'
        include_clauses[1] = ' LEFT JOIN players ON stats.Player = players.Name'
    if min_height != None or max_height != None:
        include_clauses[0] = include_clauses[0] + ', players.Height_Inches'
        include_clauses[1] = ' LEFT JOIN players ON stats.Player = players.Name'
    if max_age != None or min_age != None:
        include_clauses[0] = include_clauses[0] + ', players.Age'
        include_clauses[1] = ' LEFT JOIN players ON stats.Player = players.Name'
    if positions != None:
        include_clauses[0] = include_clauses[0] + ', players.Position'
        include_clauses[1] = ' LEFT JOIN players ON stats.Player = players.Name AND stats.TeamID = players.TeamID'
        by_team_clause = ', TeamID'

    if include != 'yes':
        include_clauses = ['','']

    if len(present_clauses) > 0:
        where_clauses = 'WHERE '+ ' AND '.join(present_clauses)

    if type_stats == 'per_game':
        full_query = ("SELECT DISTINCT stats.*" + f"{include_clauses[0]}" + " FROM (SELECT Player" + f"{by_team_clause}{by_win_loss_clause}," + " COUNT(Player) AS GP, SUM(Starter) AS GS, ROUND(SUM(MP)/COUNT(Player), 1) AS MP,"
                    " ROUND(SUM(FG)/COUNT(Player), 1) AS FG, ROUND(SUM(FGA)/COUNT(Player), 1) AS FGA, ROUND(SUM(3FG)/COUNT(Player), 1) AS 3FG," 
                    " ROUND(SUM(3FGA)/COUNT(Player), 1) AS 3FGA, ROUND(SUM(FT)/COUNT(Player), 1) AS FT, ROUND(SUM(FTA)/COUNT(Player), 1) AS FTA,"
                    " ROUND(SUM(ORB)/COUNT(Player), 1) AS ORB, ROUND(SUM(DRB)/COUNT(Player), 1) AS DRB, ROUND(SUM(TRB)/COUNT(Player), 1) AS TRB,"
                    " ROUND(SUM(AST)/COUNT(Player), 1) AS AST, ROUND(SUM(STL)/COUNT(Player), 1) AS STL, ROUND(SUM(BLK)/COUNT(Player), 1) AS BLK,"
                    " ROUND(SUM(TOV)/COUNT(Player), 1) AS TOV, ROUND(SUM(PF)/COUNT(Player), 1) AS PF, ROUND(SUM(PTS)/COUNT(Player), 1) AS PTS,"
                    " ROUND(SUM(Plus_Minus)/COUNT(Player), 1) AS Plus_Minus FROM gamestats "
                    f"{where_clauses}" + "GROUP BY Player" + f"{by_team_clause}" + f"{by_win_loss_clause}) AS stats" + include_clauses[1])

    if type_stats == 'totals':
        full_query= ("SELECT DISTINCT stats.*" + f"{include_clauses[0]}" + " FROM (SELECT Player" + f"{by_team_clause}{by_win_loss_clause}," + " COUNT(Player) AS GP, SUM(Starter) AS GS, SUM(MP) AS MP, SUM(FG) AS FG, SUM(FGA) AS FGA,"
                    " SUM(3FG) AS 3FG, SUM(3FGA) AS 3FGA, SUM(FT) AS FT, SUM(FTA) AS FTA, SUM(ORB) AS ORB, SUM(DRB) AS DRB,"
                    " SUM(TRB) AS TRB, SUM(AST) AS AST, SUM(STL) AS STL, SUM(BLK) AS BLK, SUM(TOV) AS TOV, SUM(PF) AS PF, "
                    " SUM(PTS) AS PTS, SUM(Plus_Minus) AS Plus_Minus FROM gamestats "
                    f"{where_clauses}" + " GROUP BY Player" + f"{by_team_clause}" + f"{by_win_loss_clause}) AS stats" + include_clauses[1])
        
    if type_stats == 'all_games':
        full_query= ("SELECT DISTINCT stats.*" + f"{include_clauses[0]}" + ", games.Date" + " FROM (SELECT TeamID, GameID, Player, Starter AS Start, MP, FG, FGA, 3FG, 3FGA, FT, FTA, ORB, DRB, TRB, AST, STL, BLK, TOV, PF, PTS, Plus_Minus" + f"{by_win_loss_clause}" + " FROM gamestats "
                    f"{where_clauses}) AS stats" + include_clauses[1] + " LEFT JOIN games ON stats.GameID = games.gameID")
    return full_query

splits(max_age = 30, min_age=30)


def stat_sorter(MP = '>0', FG= None , FGA= None, FG3= None, FGA3 = None, FT= None, FTA= None, ORB= None, DRB= None, TRB= None, AST= None, STL= None, BLK= None,
                   TOV = None, PF = None, PTS = None, W_L = None, Plus_Minus = None ,**kwargs):
    local_items = locals().items()
    where_clauses = [(x[0] + ' ' + str(x[1])) for x in local_items if x[0] != 'kwargs' and x[1] != None]
    where_clauses = ' AND '.join(where_clauses).replace('FG3','3FG').replace('FGA3','3FGA')
    per_game = splits(**kwargs)
    return (f'SELECT * FROM ({per_game}) sub WHERE ' + where_clauses)


def stats_for_b2b(team = None, type_stats = 'per_game', not_in = None, **kwargs):
    initial_query = splits(teams = team, type_stats = type_stats, **kwargs)
    inner_team_query = ""
    not_query = ''
    if not_in == 'yes': not_query = 'NOT '
    inner_team_query = (f"(SELECT GameID FROM Games WHERE HOME = '{team}' OR AWAY = '{team}') AND Date " + f"{not_query}" + "IN (SELECT DATE_ADD(Date, INTERVAL -1 DAY)"
                             f" FROM Games WHERE Home = '{team}' OR Away = '{team}'))")
    full_query = initial_query.split('GROUP BY')[0] + " AND GameID IN (SELECT GameID FROM Games WHERE GameID IN" + inner_team_query + "GROUP BY" + initial_query.split('GROUP BY')[1]
    if type_stats == 'all_games':
        full_query = initial_query + " AND GameID IN (SELECT GameID FROM Games WHERE GameID IN" + inner_team_query
    return full_query 


def multiple_year_query(seasons_start, seasons_end = None, **kwargs):
    """Query that returns pandas Dataframe containing desired statistics, sortable by a number of different keyword options.

    season_start - string - ex.('2010') Beginning of time span of stats search, corresponding to year that season begins
    season_end - string - ex.('2010') End of time span for stats search, corresponding to year that season ends. If left blank, only season of season_start will be included
    type_stats - 'per_game', 'all_games', 'totals' - whether you would like to return per_game averages, all game box scores, or summed total stats. Default is 'per_game'
    teams - string or list of strings of full team names ex.('Golden State Warriors'). Default is all teams
    players - string or list of strings of full player names ex.('Lebron James'). Default is all players
    positions - string or list of strings of positions, options are 'PG', 'SG', 'SF', 'PF', 'C'
    opponents - string or list of strings of full team names for stats against given opponent
    min_weight - int or string for minimum weight for player searched
    max_weight - int or string for maximum weight for player searched
    min_age - int or string for minimum age for player searched
    max_age - int or string for maximum age for player searched
    min_height - int or string for minimum height for player searched
    max_height - int or string for maximum height for player searched None,
    start_date - date string ex('2012-12-12') for start of search frame
    end_date - date string for end of search frame
    Home - 'Home' for home games, 'Away' for away, default is None for both
    win_loss - 'yes' seperates averages by win_loss, and 'yes' with include includes win_loss column in final datagrame
    by_team - 'yes' to include different teams for players who have been traded within season
    include - 'yes' to include height, weight, age, or position in final dataframe. Default is 'no'
    MP = '>0', FG= None , FGA= None, FG3= None, FGA3 = None, FT= None, FTA= None, ORB= None, DRB= None, TRB= None, AST= None, STL= None, BLK= None,
                   TOV = None, PF = None, PTS = None, W_L = None, Plus_Minus = None 
    Boolean strings to search by minimum or maximum stats for example '>20' in PTS for more than 20 points.
    """
    seasons_list = []
    if seasons_end == None:
        seasons_list.append("USE nbaseason" + str(seasons_start) + '_' + str(int(seasons_start) + 1)[2:])
    else:
        for season in range(int(seasons_start), int(seasons_end)):
            seasons_list.append("USE nbaseason" + str(season) + '_' + str(int(season) + 1)[2:])
    connection = connector.connect(user = user, password = password)
    cursor = connection.cursor()
    df = pd.DataFrame()
    for use_year in seasons_list:
        cursor.execute(use_year)
        season_df = pd.read_sql(stat_sorter(**kwargs), connection)
        season_df['Season'] = use_year[-7:]
        df = pd.concat([df, season_df])
    connection.close()
    return df
