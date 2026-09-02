import duckdb

con = duckdb.connect('transform/nba/nba.duckdb')

# Example queries

# list tables
print("Tables in the database:")
print(con.execute("SHOW TABLES").df())

# Game summaries
print("Game summaries:")
print(con.execute("SELECT * FROM game_summary").df())

# Top scorers
print("Top scorers:")
print(con.execute("SELECT player_name, team_tricode, points_scored FROM player_game_stats ORDER BY points_scored DESC LIMIT 10").df())

# Games that went to overtime
print("Games that went to overtime:")
print(con.execute("SELECT game_id, final_score_home, final_score_away, periods_played FROM game_summary WHERE went_to_overtime = true").df())