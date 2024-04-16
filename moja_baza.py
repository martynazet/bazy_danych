import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_connection_in_memory():
    """create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(":memory:")
        print(f"Connected, sqlite version: {sqlite3.version}")
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    "parm sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_producer(conn, producer):
    """
    Add a new producer into the Marka table
    :param conn:
    :param producer:
    :return: producer id
    """
    sql = '''INSERT INTO Marka(nazwa, kraj_prod)
             VALUES(?,?)'''
    cur = conn.cursor()
    cur.execute(sql, producer)
    conn.commit()
    return cur.lastrowid


def add_model(conn, model):
    """
    Create a new task into the Model table
    :param conn:
    :param model:
    :return: model id
    """
    sql = '''INSERT INTO Model (producer_id, nazwa, rok_prod, seria)
            VALUES (?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, model)
    conn.commit()
    return cur.lastrowid


def select_task_by_year(conn, year):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param status:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM Model WHERE rok_prod=?", (year,))
    rows = cur.fetchall()
    return rows


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    """
    update status, begin_date, and end date of a task
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f'''UPDATE {table}
            SET {parameters}
            WHERE id = ?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f'''DELETE FROM {table} WHERE {q}'''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")


def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f'''DELETE FROM {table}'''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")


if __name__ == '__main__':

    create_producer_sql = """
    CREATE TABLE IF NOT EXISTS Marka (
    id INTEGER PRIMARY KEY NOT NULL,
    nazwa text(20),
    kraj_prod text(25)
    )"""

    create_model_sql = """
    CREATE TABLE IF NOT EXISTS Model (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    producer_id int,
    nazwa text(25),
    rok_prod int,
    seria text(20),
    FOREIGN KEY(producer_id) REFERENCES Marka(id)
    )"""

    db_file = "my_database.db"

    conn = create_connection(db_file)
    if conn is not None:
        execute_sql(conn, create_producer_sql)
        execute_sql(conn, create_model_sql)

    marka = [("Opel", "Niemcy"), ("Kia", "Korea Południowa"), ("Volkswagen", "Niemcy"), ("Toyota", "Japonia")]
    for i in marka:
        add_producer(conn, i)

    model = [(1, "Astra F", 2000, "Sedan 1.4"), (2, "Rio", 2020, "1.2"), (3, "Passat B6", 2008, "1.9 TDI"), (4, "Aygo", 2010, "1.0")]
    for i in model:
        add_model(conn, i)

    update(conn, "Model", 4, rok_prod=2011)

    models2008 = select_task_by_year(conn, 2008)
    for model in models2008:
        print(model)
    
    select_niemcy = select_where(conn, "Marka", kraj_prod="Niemcy")
    for i in select_niemcy:
        print(i)
   
