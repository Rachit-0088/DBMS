import mysql.connector
from mysql.connector import Error
from datetime import date, timedelta
import random

# --- APNE MYSQL CONNECTION KI JAANKARI YAHAN DAALEIN ---
DB_CONFIG = {
    'user': 'root',
    'password': 'rachit@2005',
    'host': '127.0.0.1',
    'database': '' # Shuruaat mein database ka naam khaali rakhein
}

DB_NAME = 'advanced_customer_db'

def create_server_connection():
    try:
        connection = mysql.connector.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host']
        )
        print("MySQL Database se connection safal raha")
        return connection
    except Error as e:
        print(f"Connection Error: '{e}'")
        return None

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database safaltapoorvak ban gaya")
    except Error as e:
        print(f"Database banane mein error: '{e}'")
    finally:
        cursor.close()

def create_db_connection():
    try:
        connection = mysql.connector.connect(**{**DB_CONFIG, 'database': DB_NAME})
        print(f"'{DB_NAME}' database se connection safal raha")
        return connection
    except Error as e:
        print(f"Database se connect hone mein error: '{e}'")
        return None

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query safaltapoorvak execute hui")
    except Error as e:
        print(f"Query execute karne mein error: '{e}'")
    finally:
        cursor.close()

def setup_database():
    # Step 1: Server se connect karke database banayein
    server_conn = create_server_connection()
    if not server_conn: return
    create_database(server_conn, f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    server_conn.close()

    # Step 2: Naye database se connect karein
    db_conn = create_db_connection()
    if not db_conn: return
    
    # Step 3: Tables ko drop karke dobara banayein (saaf shuruaat ke liye)
    execute_query(db_conn, "DROP TABLE IF EXISTS transactions;")
    execute_query(db_conn, "DROP TABLE IF EXISTS customers;")

    create_customers_table = """
    CREATE TABLE customers (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      age INT,
      gender VARCHAR(50),
      region VARCHAR(255),
      income_level VARCHAR(50),
      join_date DATE
    );
    """
    
    create_transactions_table = """
    CREATE TABLE transactions (
      transaction_id INT AUTO_INCREMENT PRIMARY KEY,
      customer_id INT,
      amount DECIMAL(10, 2),
      cost_of_goods DECIMAL(10, 2),
      date DATE,
      product_category VARCHAR(255),
      payment_method VARCHAR(255),
      discount_used BOOLEAN,
      FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
    );
    """
    execute_query(db_conn, create_customers_table)
    execute_query(db_conn, create_transactions_table)

    # Step 4: Sample data daalein
    cursor = db_conn.cursor()
    
    customers_data = [
      ('Amit Kumar', 28, 'Male', 'Delhi', 'Medium', date(2023, 1, 15)),
      ('Priya Sharma', 35, 'Female', 'Mumbai', 'High', date(2022, 11, 20)),
      ('Rajesh Singh', 45, 'Male', 'Bangalore', 'High', date(2023, 3, 10)),
      ('Sunita Devi', 22, 'Female', 'Kolkata', 'Low', date(2023, 6, 5)),
      ('Vikram Rathore', 52, 'Male', 'Chennai', 'Medium', date(2021, 5, 25)),
      ('Anjali Mehta', 29, 'Female', 'Pune', 'Medium', date.today() - timedelta(days=15)),
      ('Sandeep Verma', 38, 'Male', 'Hyderabad', 'High', date.today() - timedelta(days=120)),
    ]
    
    customer_query = "INSERT INTO customers (name, age, gender, region, income_level, join_date) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(customer_query, customers_data)
    db_conn.commit()

    transactions_data = [
        (1, 12000.00, 9000.00, date(2023, 2, 1), 'Electronics', 'Credit Card', False),
        (2, 75000.00, 60000.00, date(2023, 2, 5), 'Jewellery', 'Credit Card', True),
        (3, 35000.00, 28000.00, date(2023, 3, 12), 'Electronics', 'UPI', False),
        (1, 5000.00, 3500.00, date(2023, 4, 20), 'Groceries', 'UPI', True),
        (4, 8000.00, 6000.00, date(2023, 6, 8), 'Fashion', 'Cash', False),
        (2, 25000.00, 20000.00, date(2023, 7, 15), 'Fashion', 'Credit Card', False),
        (5, 18000.00, 15000.00, date(2023, 8, 1), 'Home Appliances', 'Net Banking', False),
        (6, 2200.00, 1800.00, date.today() - timedelta(days=10), 'Groceries', 'UPI', False),
        (7, 400.00, 300.00, date.today() - timedelta(days=100), 'Books', 'Cash', True),
    ]

    transaction_query = "INSERT INTO transactions (customer_id, amount, cost_of_goods, date, product_category, payment_method, discount_used) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(transaction_query, transactions_data)
    db_conn.commit()
    
    print("Sample data safaltapoorvak daal diya gaya hai")
    cursor.close()
    db_conn.close()

if __name__ == '__main__':
    setup_database()

