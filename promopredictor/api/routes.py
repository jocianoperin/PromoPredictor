from flask import Flask, jsonify
from ..db.db_config import get_db_connection
from ..src.promotions.promotions import identify_promotions

app = Flask(__name__)

@app.route('/promotions', methods=['GET'])
def get_promotions():
    conn = get_db_connection()
    promotions = identify_promotions(conn)
    conn.close()
    return jsonify(promotions)
