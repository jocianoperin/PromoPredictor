from flask import Flask, jsonify
from db.db_config import get_db_connection
from db.db_operations import PromotionsDB

app = Flask(__name__)

@app.route('/promotions', methods=['GET'])
def get_promotions():
    conn = get_db_connection()
    try:
        promotions_db = PromotionsDB(conn)
        promotions = promotions_db.get_all_promotions()
        return jsonify(promotions)
    finally:
        if conn.is_connected():
            conn.close()
