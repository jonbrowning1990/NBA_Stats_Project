import key_functions
import mysql.connector as connector
import time

while True:
    year = input('What season would you like an NBA Boxscore database of? \nPlease input a year in "YYYY" format, with your selection corresponding to the year the season concluded, ie. "2002" for the 2001-2002 NBA season.\n')
    if not year in[str(x) for x in range(1975, 2025)]:
        print("Sorry, that is not a valid year. Please try again.")
        continue
    else:
        break

print(f"Creating SQL database for box score statistsics for the {int(year) -1}-{int(year)} NBA season. \nThis will take a few hours as the source site (basketballreference.com) will deny service if hit with too many html requests in too short a time.")

key_functions.create_initial_table_sql(year)

key_functions.insert_games_and_teams(year)

time.sleep(60)

key_functions.update_all_dates(year)

key_functions.update_gamestats_wins_losses(year)

print(f"All done! Your mysql database for the {int(year) -1}-{int(year)} NBA season is complete!")