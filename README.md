# NBA_Stats_Project
This repository contains the code I created to scrape, transform, and aggregate NBA Box Score statistics and upload them to into SQL databases.

## Project Conception
As a basketball fan I grew up watching the Washinigton Wizards and have fond memories of the 2003-04 Washington Wizards season especially. I thought it good practice to scrape the data from that season and compile my own table with the stats from all of that year's games. I created some code to scrape the box score data for every game, clean it of missing data or unwanted columns, and format it for statistical analysis.
I realized this could be scaled up to scrape data for the entire league including information on all the teams, players, games, and individuals' statistics for each game played. I decided to create code to build a MySQL database to store all this data and a simple execute script that could be run to build an entire season's database.

## Process
First, I considered all the information that I would want to store and potentially analyze, and I created a schema with linked tables for players, teams, games, and game statistics. 
I worked on code that would progress through an entire season, locating the necessary URLs to query for each game played, and scrape the html for the box score data. 
I also created functions to pull player data i.e. height, weight, etc., and information on individual games such as date, time, and attendence.
This data was then loaded into the database with functions using a python mySQL connection and insert/update queries compiled string functions.
Finally I made an execution script that would run and call the necessary functions to create the entire database for a chosen year.
I debugged the code so that it could be used to compile a database from any of the NBA seasons from the last 30 years.
I next constructed a script with python functions that could be used to execute complex SQL queries with features to pull data, filter on statistics, player or game attributes, sum or average player statistics, and track players or trends over multiple years.

## Skills Used
Python, MySQL, Extract/Transform/Load processes, Web Scraping, HTML Parsing, Regex Functions, Schema Creation, Database Clients, SQL Query Creation (Including Create, Update, Alter, Select, Join, Group By, etc.)

## File Contents
initial_tables.sql - SQL script that creates empty database schema to be populated with data for a given year.

key_functions.py - Key functions that are called by the execution script that pull data, clean and transform it, connect to the MySQL database, and load/update the schema's tables.

box_update.py - File containing update_box function called by key_functions script which performs core task of scraping and transforming box score data from the source url.

passwords.py - Simple script containing functions which define a the user and the password for a MySQL connection. To use the code, this file must be updated to fit your connection.

create_year_database.py - Execution script which can be run to compile data for entire year. It asks for the year of the season you would like and then gets busy.

query_database.py - File containing functions to execute complex queries that can return averages, totals, or individual game stats, and sort by a wide range of options.
