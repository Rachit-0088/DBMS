from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
from datetime import date, timedelta
import re
import pandas as pd
import os

app = Flask(__name__, static_url_path='', static_folder='.')
CORS(app) 
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# --- APNE MYSQL CONNECTION KI JAANKARI YAHAN DAALEIN ---
DB_CONFIG = {
    'user': 'root',
    'password': 'rachit@2005',
    'host': '127.0.0.1',
    'database': 'advanced_customer_db'
}

def create_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        print(f"MySQL se connect hone mein error: {e}")
        return None

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

def execute_query(query, params=None):
    conn = create_connection()
    if not conn: return None, "Database connection fail ho gayi"
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        result = cursor.fetchall()
        return result, None
    except Error as e:
        print(f"Query Error: {e}")
        return None, str(e)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# --- Naya Endpoint: File Upload ke liye ---
@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if file:
        if not os.path.exists(UPLOAD_FOLDER):
            os.makedirs(UPLOAD_FOLDER)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(filepath)

        try:
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
            elif filepath.endswith('.xlsx'):
                df = pd.read_excel(filepath)
            else:
                os.remove(filepath)
                return jsonify({"error": "Unsupported file type. Please use .csv or .xlsx"}), 400

            df.columns = df.columns.str.lower().str.replace(' ', '_')

            conn = create_connection()
            cursor = conn.cursor()
            customers_added, transactions_added = 0, 0

            for index, row in df.iterrows():
                try:
                    # Pehle customer ko add karein
                    customer_sql = "INSERT INTO customers (name, age, gender, region, income_level, join_date) VALUES (%s, %s, %s, %s, %s, %s)"
                    customer_params = (row['name'], row['age'], row['gender'], row.get('region', 'N/A'), row['income_level'], date.today().isoformat())
                    cursor.execute(customer_sql, customer_params)
                    customer_id = cursor.lastrowid
                    customers_added += 1

                    # Ab transaction add karein
                    if 'amount' in df.columns:
                        transaction_sql = "INSERT INTO transactions (customer_id, amount, cost_of_goods, date, product_category, payment_method, discount_used) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        transaction_params = (customer_id, row['amount'], row['cost_of_goods'], row.get('date', date.today().isoformat()), row['product_category'], row['payment_method'], bool(row.get('discount_used', False)))
                        cursor.execute(transaction_sql, transaction_params)
                        transactions_added += 1
                except (Error, KeyError) as e:
                    print(f"Row {index+2} par data insert karne mein error: {e}")
                    continue
            
            conn.commit()
            cursor.close()
            conn.close()
            os.remove(filepath)

            return jsonify({"success": True, "message": f"{customers_added} customers and {transactions_added} transactions added successfully."})

        except Exception as e:
            if os.path.exists(filepath): os.remove(filepath)
            return jsonify({"error": f"File process karne mein error: {str(e)}"}), 500

    return jsonify({"error": "File upload fail ho gaya"}), 500


@app.route('/api/segment/<segment_id>')
def get_segment_data(segment_id):
    today = date.today()
    base_query = """
        SELECT c.*, T.total_revenue, T.total_profit, T.purchase_count, T.last_purchase_date, T.product_categories, T.payment_methods
        FROM customers c
        LEFT JOIN (
            SELECT
                customer_id,
                SUM(amount) AS total_revenue,
                SUM(amount - cost_of_goods) AS total_profit,
                COUNT(transaction_id) AS purchase_count,
                MAX(date) AS last_purchase_date,
                GROUP_CONCAT(DISTINCT product_category) as product_categories,
                GROUP_CONCAT(DISTINCT payment_method) as payment_methods
            FROM transactions
            GROUP BY customer_id
        ) AS T ON c.id = T.customer_id
    """
    clauses = {
        'age_18_25': "WHERE c.age BETWEEN 18 AND 25", 'age_26_35': "WHERE c.age BETWEEN 26 AND 35",
        'age_36_50': "WHERE c.age BETWEEN 36 AND 50", 'age_50_plus': "WHERE c.age > 50",
        'gender_male': "WHERE c.gender = 'Male'", 'gender_female': "WHERE c.gender = 'Female'",
        'income_high': "WHERE c.income_level = 'High'", 'income_medium': "WHERE c.income_level = 'Medium'", 'income_low': "WHERE c.income_level = 'Low'",
        'high_value': "HAVING T.total_revenue > 50000",
        'medium_value': "HAVING T.total_revenue BETWEEN 20000 AND 50000",
        'low_value': "HAVING T.total_revenue > 0 AND T.total_revenue < 20000",
        'new_customers': f"WHERE c.join_date >= '{today - timedelta(days=30)}'",
        'returning_customers': "HAVING T.purchase_count BETWEEN 2 AND 10",
        'at_risk': f"HAVING T.last_purchase_date IS NOT NULL AND T.last_purchase_date <= '{today - timedelta(days=90)}'",
    }
    
    query = f"{base_query} {clauses.get(segment_id, '')};"
    rows, error = execute_query(query)
    if error: return jsonify({"error": error}), 500
    for row in rows:
        for key, value in row.items():
            if isinstance(value, date): row[key] = value.isoformat()
    return jsonify(rows)

@app.route('/api/segment/custom', methods=['POST'])
def get_custom_segment():
    custom_filter = request.json.get('filter', '')
    if not re.match(r"^[a-zA-Z0-9_'\s=<>ANDOR.()]+$", custom_filter):
        return jsonify({"error": "Invalid characters in filter."}), 400
    base_query = """
        SELECT c.*, T.total_revenue, T.total_profit, T.purchase_count, T.last_purchase_date FROM customers c 
        LEFT JOIN (SELECT customer_id, SUM(amount) AS total_revenue, SUM(amount - cost_of_goods) AS total_profit, COUNT(transaction_id) AS purchase_count, MAX(date) AS last_purchase_date FROM transactions GROUP BY customer_id) AS T 
        ON c.id = T.customer_id
    """
    query = f"{base_query} WHERE {custom_filter};"
    rows, error = execute_query(query)
    if error: return jsonify({"error": f"SQL Error: {error}"}), 400
    for row in rows:
      for key, value in row.items():
        if isinstance(value, date): row[key] = value.isoformat()
    return jsonify(rows)

@app.route('/api/comparison', methods=['GET'])
def get_comparison_data():
    start_month, end_month = request.args.get('start'), request.args.get('end')
    query = "SELECT DATE_FORMAT(date, '%%Y-%%m') AS month, SUM(amount) as total_revenue, SUM(amount - cost_of_goods) as total_profit FROM transactions WHERE DATE_FORMAT(date, '%%Y-%%m') BETWEEN %s AND %s GROUP BY month ORDER BY month;"
    rows, error = execute_query(query, (start_month, end_month))
    if error: return jsonify({"error": error}), 500
    return jsonify(rows)

@app.route('/api/customers', methods=['POST'])
def add_customer():
    data = request.json
    sql = 'INSERT INTO customers(name, age, gender, region, income_level, join_date) VALUES(%s, %s, %s, %s, %s, %s)'
    params = (data['name'], data['age'], data['gender'], data['region'], data['income_level'], date.today().isoformat())
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()
    new_id = cursor.lastrowid
    cursor.close()
    conn.close()
    return jsonify({"success": True, "id": new_id}), 201

@app.route('/api/transactions', methods=['POST'])
def add_transaction():
    data = request.json
    sql = "INSERT INTO transactions (customer_id, amount, cost_of_goods, date, product_category, payment_method, discount_used) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    params = (data['customer_id'], data['amount'], data['cost_of_goods'], date.today().isoformat(), data['product_category'], data['payment_method'], data['discount_used'])
    conn = create_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        return jsonify({"success": True, "id": cursor.lastrowid}), 201
    except Error as e:
        return jsonify({"error": str(e)}), 400
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/customers/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM customers WHERE id = %s', (customer_id,))
    conn.commit()
    success = cursor.rowcount > 0
    cursor.close()
    conn.close()
    if success: return jsonify({"success": True})
    return jsonify({"success": False, "message": "Customer not found"}), 404

if __name__ == '__main__':
    app.run(debug=True)

