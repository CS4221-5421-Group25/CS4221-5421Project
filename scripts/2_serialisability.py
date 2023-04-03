import psycopg2

database = "cs4221"
host = "localhost"
user = "postgres"
password = "12345678"
port = "5432"

def resetDatabase():
    print("> Reset the database.")
    conn = psycopg2.connect(database = database, host = host, user = user, password = password, port = port)
    cur = conn.cursor()

    cur.execute("TRUNCATE warehouse;")
    cur.execute("INSERT INTO warehouse VALUES(1, 'Alice', 22, 2000, 'Singapore');")
    cur.execute("INSERT INTO warehouse VALUES(2, 'Bob', 24, 1000, 'Indonesia');")
    cur.execute("INSERT INTO warehouse VALUES(3, 'Caruso', 20, 3000, 'Indonesia');")

    conn.commit()

    cur.close()
    conn.close()


def ReadUncommitted_DirtyRead(conn1, conn2):
    resetDatabase()
    print("> ReadUncommitted_DirtyRead: This function shows ReadUncommitted Level can prevent Dirty Read.")

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    # set isolation level
    cur1.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
    cur2.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")

    # T2 perform query before T1 updates
    print("> Before T1's update, T2 select * from warehouse where w_id = 1:")
    cur2.execute("SELECT * FROM warehouse WHERE w_id = 1;")
    print(cur2.fetchall())

    # T1 updates, but has not committed
    print("> T1 update w_name to 'Johnson' where w_id = 1.")
    cur1.execute("UPDATE warehouse SET w_name = 'Johnson' WHERE w_id = 1;")

    # T2 perform query aftere T1 updates
    print("> After T1's update, T2 select * from warehouse where w_id = 1:")
    cur2.execute("SELECT * FROM warehouse WHERE w_id = 1;")
    print(cur2.fetchall())

    # close cursor
    cur1.close()
    cur2.close()

def ReadCommitted_NonrepeatableRead(conn1, conn2):
    resetDatabase()
    print("> ReadCommitted_NonrepeatableRead: "
          "This function shows ReadCommitted Level cannot prevent Non-repeatable Read.")

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    # set isolation level
    cur1.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
    cur2.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    # T2 perform query before T1 updates and commits
    print("> Before T1's update & commits, T2 select * from warehouse where w_id = 1:")
    cur2.execute("SELECT * FROM warehouse WHERE w_id = 1;")
    print(cur2.fetchall())

    # T1 updates and commits
    print("> T1 update w_salary where w_id = 1, and commits:")
    cur1.execute("UPDATE warehouse SET w_salary = w_salary + 1000 WHERE w_id = 1 RETURNING *;")
    print(cur1.fetchall())
    conn1.commit()

    # T2 perform query aftere T1 updates
    print("> After T1's update & commit, T2 select * from warehouse where w_id = 1:")
    cur2.execute("SELECT * FROM warehouse WHERE w_id = 1;")
    print(cur2.fetchall())

    # close cursor
    cur1.close()
    cur2.close()

    return

def ReadCommitted_Phantoms(conn1, conn2):
    resetDatabase()
    print("> ReadCommitted_Phantoms: This function shows ReadCommitted Level cannot prevent Phantoms.")

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    # set isolation level
    cur1.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
    cur2.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    # T2 perform query before T1 updates and commits
    print("> Before T1's insert & commits, T2 select * from warehouse where w_age > 21")
    cur2.execute("SELECT * FROM warehouse WHERE w_age > 21;")
    print(cur2.fetchall())

    # T1 updates and commits
    print("> T1 inserts 1 record and commits:")
    cur1.execute("INSERT INTO warehouse VALUES(4,'Daniel',25,4000,'Singapore') RETURNING *;")
    print(cur1.fetchall())
    conn1.commit()

    # T2 perform query aftere T1 updates
    print("> After T1's insert & commit, T2 select * from warehouse where w_age > 21:")
    cur2.execute("SELECT * FROM warehouse WHERE w_age > 21;")
    print(cur2.fetchall())

    # close cursor
    cur1.close()
    cur2.close()

    return

def RepeatableRead_NonrepeatableRead(conn1, conn2):
    resetDatabase()
    print("> RepeatableRead_NonrepeatableRead: "
          "This function shows Repeatable Read Level can prevent Non-repeatable Read.")

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    # set isolation level
    cur1.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
    cur2.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")

    # T2 perform query before T1 updates and commits
    print("> Before T1's update & commits, T2 select * from warehouse where w_id = 1:")
    cur2.execute("SELECT * FROM warehouse WHERE w_id = 1;")
    print(cur2.fetchall())

    # T1 updates and commits
    print("> T1 update w_salary where w_id = 1, and commits:")
    cur1.execute("UPDATE warehouse SET w_salary = w_salary + 1000 WHERE w_id = 1 RETURNING *;")
    print(cur1.fetchall())
    conn1.commit()

    # T2 perform query aftere T1 updates
    print("> After T1's update & commit, T2 select * from warehouse where w_id = 1:")
    cur2.execute("SELECT * FROM warehouse WHERE w_id = 1;")
    print(cur2.fetchall())

    # close cursor
    cur1.close()
    cur2.close()

    return

def RepeatableRead_Phantoms(conn1, conn2):
    resetDatabase()
    print("> ReadCommitted_Phantoms: This function shows ReadCommitted Level can prevent Phantoms.")

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    # set isolation level
    cur1.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
    cur2.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")

    # T2 perform query before T1 updates and commits
    print("> Before T1's insert & commits, T2 select * from warehouse where w_age > 21")
    cur2.execute("SELECT * FROM warehouse WHERE w_age > 21;")
    print(cur2.fetchall())

    # T1 updates and commits
    print("> T1 inserts 1 record and commits:")
    cur1.execute("INSERT INTO warehouse VALUES(4,'Daniel',25,4000,'Singapore') RETURNING *;")
    print(cur1.fetchall())
    conn1.commit()

    # T2 perform query aftere T1 updates
    print("> After T1's insert & commit, T2 select * from warehouse where w_age > 21:")
    cur2.execute("SELECT * FROM warehouse WHERE w_age > 21;")
    print(cur2.fetchall())

    # close cursor
    cur1.close()
    cur2.close()

    return

def RepeatableRead_SerializationAnomaly(conn1, conn2):
    resetDatabase()
    print("> RepeatableRead_SerializationAnomaly: "
          "This function shows RepeatableRead Level cannot prevent SerializationAnomaly.")

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    # set isolation level
    cur1.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
    cur2.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")

    # insert
    print("> T1 insert a record into warehouse:")
    cur1.execute("INSERT INTO warehouse (w_id, w_name,w_age,w_salary,w_country) "
                 "SELECT 4,'sum',0,SUM(w_salary),'All' FROM warehouse RETURNING *;")
    print(cur1.fetchall());


    print("> T2 insert a record into warehouse:")
    cur2.execute("INSERT INTO warehouse (w_id, w_name,w_age,w_salary,w_country) "
                 "SELECT 5,'sum',0,SUM(w_salary),'All' FROM warehouse RETURNING *;")
    print(cur2.fetchall());

    conn1.commit()
    conn2.commit()

    print("> After T1 & T2's insert, the warehouse:")
    cur1.execute("SELECT * FROM warehouse;")
    print(cur1.fetchall())

    # close cursor
    cur1.close()
    cur2.close()
    return


def Serializable_SerializationAnomaly(conn1, conn2):
    resetDatabase()
    print("> Serializable_SerializationAnomaly: "
          "This function shows Serializable Level can prevent SerializationAnomaly.")

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    # set isolation level
    cur1.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    cur2.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    # insert
    print("> T1 insert a record into warehouse:")
    cur1.execute("INSERT INTO warehouse (w_id, w_name,w_age,w_salary,w_country) "
                 "SELECT 4,'sum',0,SUM(w_salary),'All' FROM warehouse RETURNING *;")
    print(cur1.fetchall());


    print("> T2 insert a record into warehouse:")
    cur2.execute("INSERT INTO warehouse (w_id, w_name,w_age,w_salary,w_country) "
                 "SELECT 5,'sum',0,SUM(w_salary),'All' FROM warehouse RETURNING *;")
    print(cur2.fetchall());

    print("> T1 commits.")
    conn1.commit()
    print("> Try T2 commits:")
    try:
        conn2.commit()
    except psycopg2.errors.SerializationFailure as e:
        print("> ERROR: could not serialize access due to read/write dependencies among transactions \n"+
              "DETAIL:  Reason code: Canceled on identification as a pivot, during commit attempt. \n"+
              "HINT:  The transaction might succeed if retried.")

    # close cursor
    cur1.close()
    cur2.close()
    return

def helpConcurrentTransaction():
    resetDatabase()
    helpMessage = "----------------------------------------\n" \
                  "Free mode: You can freely input SQL to 2 transactions.\n" \
                  "Please follow the format:\n" \
                  "1 [SQL] : Execute SQL in Transaction 1\n" \
                  "2 [SQL] : Execute SQL in Transaction 2\n" \
                  "3 : Commit transaction 1\n" \
                  "4 : Commit transaction 2\n" \
                  "0 : Exit\n" \
                  "----------------------------------------"
    print(helpMessage)

def concurrentTransaction(conn1, conn2):
    helpConcurrentTransaction()

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    while(True):

        print("> Please input command:")

        userCommand = input().split()

        try:
            commandType = int(userCommand[0])
        except ValueError:
            print("> Invalid command index, please input again!")
            continue
        commandArgs = ' '.join(userCommand[1:])

        if commandType == 0:
            return
        elif commandType == 1:
            try:
                cur1.execute(commandArgs)
            except psycopg2.errors.SyntaxError as e:
                print("> Syntax Error, please input again!")
                continue
            except psycopg2.ProgrammingError as ee:
                print("> Programming Error, please input again!")
                continue

            try:
                print(cur1.fetchall())
            except psycopg2.ProgrammingError as e:
                continue

        elif commandType == 2:

            try:
                cur2.execute(commandArgs)
            except psycopg2.errors.SyntaxError as e:
                print("> Syntax Error, please input again!")
                continue
            except psycopg2.ProgrammingError as ee:
                print("> Programming Error, please input again!")
                continue

            try:
                print(cur2.fetchall())
            except psycopg2.ProgrammingError as e:
                continue

        elif commandType == 3:
            try:
                print("> T1 commits.")
                conn1.commit()
            except psycopg2.errors.SerializationFailure as e:
                print("> Cannot commit T1 due to SerializationFailure!")

        elif commandType == 4:
            try:
                print("T2 commits.")
                conn2.commit()
            except psycopg2.errors.SerializationFailure as e:
                print("> Cannot commit T1 due to SerializationFailure!")

        else:
            print("> Invaild command, please input again!")
            continue



def main():
    conn1 = psycopg2.connect(database = database,
                            host = host,
                            user = user,
                            password = password,
                            port = port)
    conn2 = psycopg2.connect(database = database,
                            host = host,
                            user = user,
                            password = password,
                            port = port)


    print('''
----------------------------------------
1. ReadUncommitted_DirtyRead
2. ReadCommitted_NonrepeatableRead
3. ReadCommitted_Phantoms
4. RepeatableRead_NonrepeatableRead
5. RepeatableRead_Phantoms
6. RepeatableRead_SerializationAnomaly
7. Serializable_SerializationAnomaly
8. Free mode
----------------------------------------
> Enter number 1-8 to start:''')
    n = int(input())
    if n == 1:
        ReadUncommitted_DirtyRead(conn1, conn2)
    elif n == 2:
        ReadCommitted_NonrepeatableRead(conn1, conn2)
    elif n == 3:
        ReadCommitted_Phantoms(conn1, conn2)
    elif n == 4:
        RepeatableRead_NonrepeatableRead(conn1, conn2)
    elif n == 5:
        RepeatableRead_Phantoms(conn1, conn2)
    elif n == 6:
        RepeatableRead_SerializationAnomaly(conn1, conn2)
    elif n == 7:
        Serializable_SerializationAnomaly(conn1, conn2)
    elif n == 8:
        concurrentTransaction(conn1, conn2)

    conn1.close()
    conn2.close()


if __name__ == '__main__':
    main()