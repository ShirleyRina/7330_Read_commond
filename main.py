import csv
# Inside main.py
from database import create_server_connection

import commands

def process_command(connection, command, params):
    if command == 'e':
        commands.create_tables(connection)
    elif command == 'r':
        commands.clear_all_data(connection)
    elif command == 'p':
        # Expecting params to be: ID, Name, Birthdate (yyyymmdd), Rating, State
        commands.add_player(connection, params)
    elif command == 'm':
        # Expecting params for a completed match
        commands.add_match(connection, params)
    elif command == 'n':
        # Expecting params for a future match
        commands.add_future_match(connection, params)
    elif command == 'c':
        # Completing a match that was previously added as 'to be played'
        commands.add_complete_match(connection, params)
    elif command == 'P':
        # Querying player info
        commands.get_player_info(connection, params[0])
    elif command == 'A':
        # Player's win-loss record
        commands.player_win_loss_record(connection, params[0])
    elif command == 'D':
        # Matches in a date range
        commands.list_matches_in_date_range(connection, params[0], params[1])
    elif command == 'M':
        # Matches played by a specific player
        commands.list_player_matches(connection, params[0])
    else:
        print(f"Command {command} is not recognized.")

def process_csv_file(filename, connection):
    with open(filename, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            command = row[0] # Convert command to lowercase to standardize
            params = row[1:]
            process_command(connection, command, params)



def main():
    connection = create_server_connection('localhost', 'cs5330', 'pw5330', 'dbprog')
    if connection is not None:
        print("Database connection established. Please enter the CSV file name.")
        filename = input("CSV Filename: ")
        process_csv_file(filename, connection)
        connection.close()
        print("Database connection closed.")
    else:
        print("Failed to connect to the database.")
        

if __name__ == "__main__":
    main()
