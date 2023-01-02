# %%
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# %%
def create_database():

    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    - The connection strings below are for my docker container
    - you will need to replace all connection string with your own for your docker container or localhost for this to work
    """

    # connect to default database
    conn = psycopg2.connect(
    dbname="default_db",
    password="password",
    user="aadehamid",
    host= "172.19.0.2",
    port="5432")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
    dbname="sparkifydb",
    password="password",
    user="aadehamid",
    host= "172.19.0.2",
    port="5432")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn

# %%
def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        # conn.commit()

#%%

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        # conn.commit()

# %%

def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    cur.close()

if __name__ == "__main__":
    main()

# %%
