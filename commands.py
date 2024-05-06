#import mysql.connector
import datetime
from database import execute_query
from mysql.connector import Error

def create_tables(connection):
    player_table_query = """
    CREATE TABLE IF NOT EXISTS Player (
        ID INT PRIMARY KEY,
        Name VARCHAR(255) UNIQUE NOT NULL,
        Birthdate DATE NOT NULL,
        Rating INT NOT NULL CHECK (Rating >= 100),
        State CHAR(2) NOT NULL
    );
    """
    matches_table_query = """
    CREATE TABLE IF NOT EXISTS Matches (
        HostID INT,
        GuestID INT,
        Start DATETIME,
        End DATETIME,
        Hostwin BOOLEAN,
        PreRatingHost INT,
        PostRatingHost INT,
        PreRatingGuest INT,
        PostRatingGuest INT,
        FOREIGN KEY (HostID) REFERENCES Player(ID),
        FOREIGN KEY (GuestID) REFERENCES Player(ID)
    );
    """
    print("Attempting to create tables.")
    execute_query(connection, player_table_query)
    execute_query(connection, matches_table_query)
    print("Tables created (if they didn't exist already).\n")

# def clear_all_data(connection):
#     clear_player = "DELETE FROM Player;"
#     clear_matches = "DELETE FROM Matches;"
#     print("Attempting to clear all data.")
#     execute_query(connection, clear_player)
#     execute_query(connection, clear_matches)
#     print("All data from Player and Matches tables has been cleared.")
def clear_all_data(connection):
    try:
        # 删除 matches 表中的数据
        execute_query(connection, "DELETE FROM Matches;")
        # 删除 player 表中的数据
        execute_query(connection, "DELETE FROM Player;")
        print("All data from Player and Matches tables has been cleared.\n")
    except Error as err:
        print(f"Error: '{err}'")


def add_player(connection, data):
    id, name, birthdate, rating, state = data
    state = state.strip()  # Trim whitespace from the state
    if len(state) > 2:
        print(f"Error: State '{state}' is too long. It must be 2 characters.")
        return  # Exit the function if state is too long
    query = "INSERT INTO Player (ID, Name, Birthdate, Rating, State) VALUES (%s, %s, STR_TO_DATE(%s, '%Y%m%d'), %s, %s);"
    print(f"Attempting to add player: {data}")
    execute_query(connection, query, (id, name, birthdate, rating, state))
    print(f"Player with ID: {id} added successfully.\n")


# def add_match(connection, data):
#     query = """
#     INSERT INTO Matches (HostID, GuestID, Start, End, Hostwin, PreRatingHost, PostRatingHost, PreRatingGuest, PostRatingGuest)
#     VALUES (%s, %s, STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S'), STR_TO_DATE(%s, '%Y%m%d:%H:%i:%s'), %s, %s, %s, %s, %s);
#     """
#     print(f"Attempting to add match: {data}")
#     execute_query(connection, query, data)
#     print("Match added successfully.")
    
def add_match(connection, data):
    query = """
    INSERT INTO Matches (HostID, GuestID, Start, End, Hostwin, PreRatingHost, PostRatingHost, PreRatingGuest, PostRatingGuest)
    VALUES (%s, %s, STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S'), STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S'), %s, %s, %s, %s, %s);
    """
    print(f"Attempting to add match: {data}")
    execute_query(connection, query, data)
    print("Match added successfully.\n")


# def add_future_match(connection, data):
#     query = """
#     INSERT INTO Matches (HostID, GuestID, Start)
#     VALUES (%s, %s, STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S'));
#     """
#     print(f"Adding a future match: {data}")
#     execute_query(connection, query, data)
#     print("Future match scheduled successfully.\n")

def add_future_match(connection, data):
    try:
        # 解构参数列表
        host_id, guest_id, start = data

        # 查询将要插入的比赛是否已经存在
        check_query = """
        SELECT COUNT(*)
        FROM Matches
        WHERE HostID = %s AND GuestID = %s AND Start = STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S');
        """

        cursor = connection.cursor()

        # 检查是否已经存在相同的比赛
        cursor.execute(check_query, (host_id, guest_id, start))
        match_exists = cursor.fetchone()[0] > 0

        if match_exists:
            print(f"Match already scheduled: {data}")
            print("Input Invalid")
            return

        # 插入新的比赛信息
        query = """
        INSERT INTO Matches (HostID, GuestID, Start)
        VALUES (%s, %s, STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S'));
        """
        execute_query(connection, query, data)
        print("Future match scheduled successfully.\n")

    except Exception as err:
        print(f"Error while adding future match: {err}")
        print("Input Invalid")

    finally:
        if cursor:
            cursor.close()





def add_complete_match(connection, params):
    # 解构参数列表
    host_id, guest_id, start, end, hostwin, pre_rating_host, post_rating_host, pre_rating_guest, post_rating_guest = params

    # 检查比赛是否存在的查询
    check_query = """
    SELECT COUNT(*)
    FROM Matches
    WHERE HostID = %s AND GuestID = %s AND Start = STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S');
    """

    # 更新比赛信息的查询
    update_query = """
    UPDATE Matches
    SET End = STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S'), Hostwin = %s, 
    PreRatingHost = %s, PostRatingHost = %s, PreRatingGuest = %s, PostRatingGuest = %s
    WHERE HostID = %s AND GuestID = %s AND Start = STR_TO_DATE(%s, '%Y%m%d:%H:%i:%S');
    """

    try:
        cursor = connection.cursor()
        
        # 首先，检查比赛是否存在
        cursor.execute(check_query, (host_id, guest_id, start))
        match_exists = cursor.fetchone()[0] > 0

        if not match_exists:
            print("No matching match found. Update rejected.")
            return

        # 如果比赛存在，更新其信息
        cursor.execute(update_query, (end, hostwin, pre_rating_host, post_rating_host, pre_rating_guest, post_rating_guest, host_id, guest_id, start))  # 检查这里的参数顺序和update_query中的占位符顺序是否一致
        connection.commit()
        print("Match successfully updated.")
        
    except Exception as err:
        # 如果发生错误，回滚任何数据库更改
        #connection.rollback()
        print(f"Error while updating match: {err}")
        
    finally:
        # 关闭游标
        if cursor is not None:
            cursor.close()




def get_player_info(connection, player_id):
    query = "SELECT ID, Name, DATE_FORMAT(Birthdate, '%Y-%m-%d'), Rating, State FROM Player WHERE ID = %s;"
    print(f"Fetching information for player ID: {player_id}")
    cursor = connection.cursor()
    try:
        cursor.execute(query, (player_id,))
        result = cursor.fetchall()
        for row in result:
            print(f"{row}\n") #print all the result
    except Error as err:
        print(f"Error: '{err}'")
    finally:
        cursor.close()


# def player_win_loss_record(connection, player_id):
#     win_query = """
#     SELECT COUNT(*) FROM Matches WHERE HostID = %s AND Hostwin = TRUE OR GuestID = %s AND Hostwin = FALSE;
#     """
#     loss_query = """
#     SELECT COUNT(*) FROM Matches WHERE HostID = %s AND Hostwin = FALSE OR GuestID = %s AND Hostwin = TRUE;
#     """
#     print(f"\nFetching win/loss record for player ID: {player_id}")
#     execute_query(connection, win_query, (player_id, player_id))
#     execute_query(connection, loss_query, (player_id, player_id))

# def player_win_loss_record(connection, player_id):
#     win_query = "SELECT COUNT(*) FROM Matches WHERE HostID = %s AND Hostwin = 1;"
#     loss_query = "SELECT COUNT(*) FROM Matches WHERE GuestID = %s AND Hostwin = 0;"

#     try:
#         cursor = connection.cursor()

#         # 查询玩家赢得比赛次数
#         cursor.execute(win_query, (player_id,))
#         # 读取结果集并处理
#         win_count = cursor.fetchone()[0]

#         # 查询玩家输掉比赛次数
#         cursor.execute(loss_query, (player_id,))
#         # 读取结果集并处理
#         loss_count = cursor.fetchone()[0]

#         # 打印赢得比赛和输掉比赛次数
#         print(f"Player with ID {player_id} has won {win_count} matches and lost {loss_count} matches.")
        
#         cursor.close()  # 关闭游标
#     except Error as err:
#         print(f"Error: '{err}'")
def player_win_loss_record(connection, player_id):
    player_info_query = """
    SELECT ID, Name FROM Player WHERE ID = %s;
    """

    detailed_record_query = """
    SELECT 
        CASE 
            WHEN HostID = %s THEN GuestID 
            ELSE HostID 
        END AS OpponentID,
        (SELECT Name FROM Player WHERE ID = OpponentID) AS OpponentName,
        SUM(CASE 
                WHEN HostID = %s AND Hostwin = TRUE THEN 1 
                WHEN GuestID = %s AND Hostwin = FALSE THEN 1 
                ELSE 0 
            END) AS Wins,
        SUM(CASE 
                WHEN HostID = %s AND Hostwin = FALSE THEN 1 
                WHEN GuestID = %s AND Hostwin = TRUE THEN 1 
                ELSE 0 
            END) AS Losses
    FROM Matches
    WHERE %s IN (HostID, GuestID)
    GROUP BY OpponentID
    ORDER BY OpponentID;
    """
    
    try:
        cursor = connection.cursor()

        # Fetch the player's info
        cursor.execute(player_info_query, (player_id,))
        player_info = cursor.fetchone()
        if player_info:
            print(f"Player ID {player_info[0]}, Name of player {player_info[1]}")
        else:
            print("Player not found.")
            return

        # Fetch and print detailed win-loss record against each opponent
        cursor.execute(detailed_record_query, (player_id, player_id, player_id, player_id, player_id, player_id))
        for row in cursor.fetchall():
            opponent_id, opponent_name, wins, losses = row
            print(f"Opponent Player ID {opponent_id}, Name {opponent_name}, {wins}, {losses}")
        
        cursor.close()  # Close cursor
    except Exception as err:
        print(f"Error: '{err}'")



# def list_matches_in_date_range(connection, start_date, end_date):
#     query = """
#     SELECT Start, End, HostID, GuestID, Hostwin FROM Matches
#     WHERE Start >= STR_TO_DATE(%s, '%Y%m%d') AND End <= STR_TO_DATE(%s, '%Y%m%d')
#     ORDER BY Start, HostID;
#     """
#     print(f"Listing all matches from {start_date} to {end_date}")
#     execute_query(connection, query, (start_date, end_date))


# def list_matches_in_date_range(connection, start_date, end_date):
#     query = "SELECT * FROM Matches WHERE Start >= %s AND End <= %s;"
    
#     try:
#         cursor = connection.cursor()
#         cursor.execute(query, (start_date, end_date))
        
#         # 读取结果集并处理
#         result = cursor.fetchall()
#         for row in result:
#             print(row)  # 根据需要进一步处理结果
        
#         cursor.close()  # 关闭游标
#     except Error as err:
#         print(f"Error: '{err}'")

def list_matches_in_date_range(connection, start_date, end_date):
    # Convert the input date strings to a format that your database understands
    start_date_formatted = datetime.datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
    end_date_formatted = datetime.datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
    
    query = """
    SELECT 
        m.Start, 
        m.End, 
        ph.Name AS HostName, 
        pg.Name AS GuestName, 
        CASE WHEN m.Hostwin THEN 'H' ELSE 'G' END AS Winner
    FROM Matches m
    JOIN Player ph ON m.HostID = ph.ID
    JOIN Player pg ON m.GuestID = pg.ID
    WHERE DATE(m.Start) >= %s AND DATE(m.End) <= %s
    ORDER BY m.Start, m.HostID;
    """
    
    try:
        cursor = connection.cursor()
        # Execute the query
        cursor.execute(query, (start_date_formatted, end_date_formatted))
        
        # Fetch all the matching records
        matches = cursor.fetchall()
        
        # Check if matches are found
        if matches:
            for match in matches:
                # Adjust these lines if your database returns a different type
                start, end, host_name, guest_name, winner = match
                start_str = start.strftime('%Y%m%d')
                end_str = end.strftime('%Y%m%d')
                print(f"{start_str}, {end_str}, {host_name}, {guest_name}, {winner}")
        else:
            print("No matches found in the specified date range.")
        
        cursor.close()
    except Exception as err:
        print(f"Error: '{err}'")






def list_player_matches(connection, player_id):
    query = """
    SELECT Start, End, HostID, GuestID, Hostwin FROM Matches
    WHERE HostID = %s OR GuestID = %s
    ORDER BY Start;
    """
    print(f"Listing all matches for player ID: {player_id}")
    execute_query(connection, query, (player_id))


