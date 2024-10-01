import key_functions
import mysql.connector as connector

while True:
    year = input('What year would you like an NBA Boxscore database of? Please input a year in "YYYY" format.\n')
    if not year in[str(x) for x in range(1975, 2025)]:
        print("Sorry, that is not a valid year. Please try again.")
        continue
    else:
        break
print(year)
key_functions.create_initial_table_sql(year)
key_functions.insert_games_and_teams(year)
key_fucntions.update_all_dates(year)
key_functions.update_gamestats_wins_losses(year)
