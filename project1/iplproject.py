import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# === Connect to MySQL ===
def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='iplproject'
        )
        print(" MySQL connection established.")
        return conn
    except mysql.connector.Error as err:
        print(" MySQL connection error:", err)
        return None

# === Clean DataFrames ===
def clean_data(matches_df, deliveries_df):
    # Clean matches.csv
    matches_df.drop_duplicates(inplace=True)
    matches_df.dropna(subset=['id', 'season', 'team1', 'team2'], inplace=True)
    matches_df['date'] = pd.to_datetime(matches_df['date'], errors='coerce')
    matches_df.dropna(subset=['date'], inplace=True)

    # Clean deliveries.csv
    deliveries_df.drop_duplicates(inplace=True)
    deliveries_df.dropna(subset=['match_id', 'batsman', 'bowler'], inplace=True)
    deliveries_df['batsman_runs'] = deliveries_df['batsman_runs'].fillna(0).astype(int)

    return matches_df, deliveries_df

# === Upload CSV to MySQL (Initial Step) ===
def upload_to_mysql():
    try:
        matches_df = pd.read_csv("matches.csv")
        deliveries_df = pd.read_csv("deliveries.csv")

        # Clean the data
        matches_df, deliveries_df = clean_data(matches_df, deliveries_df)

        engine = create_engine("mysql+mysqlconnector://root:root@localhost/iplproject")

        matches_df.to_sql(name='matches', con=engine, if_exists='replace', index=False)
        deliveries_df.to_sql(name='deliveries', con=engine, if_exists='replace', index=False)

        print(" Data uploaded to MySQL.")
        return engine
    except Exception as e:
        print("Error uploading to MySQL:", e)
        return None

# === Menu ===
def show_menu():
    print("\n IPL Performance Menu")
    print("1. Total Wins by Team")
    print("2. Season-wise Wins")
    print("3. Toss Decision Impact")
    print("4. Toss Winner Match Outcome")
    print("5. Head-to-Head Stats")
    print("6. Top Run Scorers by Season")
    print("7. Top Wicket Takers by Season")
    print("8. Generate Team Reports")
    print("9. Season Winners and Runner-ups")
    print("10. Exit")

# === Menu Logic ===
def handle_choice(choice, engine):
    try:
        conn = engine.connect()

        if choice == '1':
            query = """
                SELECT winner AS Team, COUNT(*) AS Wins
                FROM matches
                WHERE winner IS NOT NULL
                GROUP BY winner
                ORDER BY Wins DESC
            """
            df = pd.read_sql(query, conn)
            print(df)
            df.plot(kind='bar', x='Team', y='Wins', color='teal', figsize=(12, 6), legend=False)
            plt.title("Total Wins by IPL Teams")
            plt.tight_layout()
            plt.show()

        elif choice == '2':
            query = """
                SELECT season, winner, COUNT(*) AS Wins
                FROM matches
                WHERE winner IS NOT NULL
                GROUP BY season, winner
                ORDER BY season, Wins DESC
            """
            df = pd.read_sql(query, conn)
            pivot = df.pivot(index='season', columns='winner', values='Wins').fillna(0)
            pivot.plot(kind='bar', stacked=True, figsize=(14, 7), colormap='tab20')
            plt.title("Season-wise Wins by Team")
            plt.tight_layout()
            plt.show()

        elif choice == '3':
            query = """
                SELECT toss_decision, winner, COUNT(*) AS Match_Wins
                FROM matches
                WHERE toss_decision IS NOT NULL AND winner IS NOT NULL
                GROUP BY toss_decision, winner
            """
            df = pd.read_sql(query, conn)
            print(df)
            pivot = df.pivot(index='toss_decision', columns='winner', values='Match_Wins').fillna(0)
            pivot.plot(kind='bar', figsize=(10, 6), colormap='Set2')
            plt.title("Match Wins Based on Toss Decision")
            plt.tight_layout()
            plt.show()

        elif choice == '4':
            total_query = "SELECT COUNT(*) AS Total FROM matches"
            win_query = "SELECT COUNT(*) AS TossWin FROM matches WHERE toss_winner = winner"

            total = pd.read_sql(total_query, conn)['Total'][0]
            toss_win = pd.read_sql(win_query, conn)['TossWin'][0]

            sizes = [toss_win, total - toss_win]
            labels = ['Toss Winner Won', 'Toss Winner Lost']
            plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['limegreen', 'salmon'])
            plt.title("Toss Impact on Match Outcome")
            plt.tight_layout()
            plt.show()
            print(f" Toss winner won the match in {toss_win} out of {total} games.")

        elif choice == '5':
            query = """
                SELECT team1, team2, winner, COUNT(*) AS Wins
                FROM matches
                WHERE team1 IS NOT NULL AND team2 IS NOT NULL AND winner IS NOT NULL
                GROUP BY team1, team2, winner
            """
            df = pd.read_sql(query, conn)
            print(df)

        elif choice == '6':
            query = """
                SELECT m.season, d.batsman, SUM(d.batsman_runs) AS TotalRuns
                FROM deliveries d
                JOIN matches m ON d.match_id = m.id
                GROUP BY m.season, d.batsman
            """
            df = pd.read_sql(query, conn)
            for season in sorted(df['season'].unique()):
                season_df = df[df['season'] == season].sort_values('TotalRuns', ascending=False).head(5)
                print(f"\n Top Run Scorers in Season {season}")
                print(season_df)
                season_df.plot(kind='bar', x='batsman', y='TotalRuns', color='gold', figsize=(8, 4), legend=False)
                plt.title(f"Top Run Scorers – Season {season}")
                plt.tight_layout()
                plt.show()

        elif choice == '7':
            query = """
                SELECT m.season, d.bowler, COUNT(*) AS Wickets
                FROM deliveries d
                JOIN matches m ON d.match_id = m.id
                WHERE d.dismissal_kind IS NOT NULL AND d.player_dismissed IS NOT NULL
                GROUP BY m.season, d.bowler
            """
            df = pd.read_sql(query, conn)
            for season in sorted(df['season'].unique()):
                season_df = df[df['season'] == season].sort_values('Wickets', ascending=False).head(5)
                print(f"\n Top Wicket Takers in Season {season}")
                print(season_df)
                season_df.plot(kind='bar', x='bowler', y='Wickets', color='steelblue', figsize=(8, 4), legend=False)
                plt.title(f"Top Wicket Takers – Season {season}")
                plt.tight_layout()
                plt.show()

        elif choice == '8':
            query = "SELECT DISTINCT winner FROM matches WHERE winner IS NOT NULL"
            teams = pd.read_sql(query, conn)['winner'].dropna().unique()
            for team in teams:
                team_query = f"SELECT * FROM matches WHERE winner = '{team}'"
                team_df = pd.read_sql(team_query, conn)
                team_df.to_csv(f"{team}_report.csv", index=False)
            print(" Team reports generated.")

        elif choice == '9':
            query = """
                SELECT season, id, team1, team2, winner, date
                FROM matches
                WHERE season IS NOT NULL AND winner IS NOT NULL
                ORDER BY season, date DESC
            """
            df = pd.read_sql(query, conn)
            final_matches = df.drop_duplicates(subset=['season'], keep='first').copy()

            final_matches['runner_up'] = final_matches.apply(
                lambda row: row['team2'] if row['team1'] == row['winner'] else row['team1'], axis=1
            )

            result_df = final_matches[['season', 'winner', 'runner_up']]
            result_df.columns = ['Season', 'Winner', 'Runner-up']

            print("\n IPL Season Winners and Runner-ups")
            print(result_df.to_string(index=False))

        elif choice == '10':
            print(" Exiting.")
            return False

        else:
            print(" Invalid option. Try again.")
    except Exception as e:
        print(" Error:", e)
    return True

# === Runner ===
def main():
    conn = connect_mysql()
    if not conn:
        return
    engine = upload_to_mysql()
    if not engine:
        return

    running = True
    while running:
        show_menu()
        choice = input("Choose an option (1–10): ")
        running = handle_choice(choice, engine)

if __name__ == "__main__":
    main()
