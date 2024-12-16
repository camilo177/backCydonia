from flask import Flask, request, jsonify
import mysql.connector
from flask_cors import CORS
import logging

app = Flask(__name__)
CORS(app)  

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]: %(message)s')

def db_connection():
    try:
        conn = mysql.connector.connect(
            user="root",
            password="Nairocontador17",
            database="habitat_warehousetest"
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Database connection failed: {err}")
        raise

# This route handles only get and post methods, this should be scalated afterwards
@app.route('/data', methods=['GET', 'POST'])
def data():
    if request.method == 'POST':

        data = request.json

        # Validate presence of all fields
        if not all(key in data for key in ('sensor_id', 'location_id', 'value')):
            return jsonify({"error": "Missing required fields (sensor_id, location_id, value)"}), 400

        value = data['value']

        try:
            conn = db_connection()
            cursor = conn.cursor()

            # Validate location_id in the table
            cursor.execute("SELECT location_id FROM dim_location WHERE location_id = %s", (data['location_id'],))
            location_exists = cursor.fetchone()

            if not location_exists:
                return jsonify({"error": "Invalid id"}), 400

            # Validate sensor_id in the table
            cursor.execute("SELECT sensor_id FROM dim_sensors WHERE sensor_id = %s", (data['sensor_id'],))
            sensor_exists = cursor.fetchone()

            if not sensor_exists:
                return jsonify({"error": "Invalid id"}), 400

            sql = """
                INSERT INTO fact_observations (sensor_id, location_id, topic_value, timestamp)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """
            cursor.execute(sql, (
                data['sensor_id'],
                data['location_id'],
                value
            ))

            conn.commit()
            cursor.close()
            conn.close()

            logging.info(f"Data saved! {data}")
            return jsonify({"status": "success", "message": "Data saved! Ready to go crew Meadaivú!"}), 201

        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")
            return jsonify({"error in the db": str(err)}), 500
        except Exception as e:
            logging.error(f"Unexpected error try and fix it mate: {e}")
            return jsonify({"error": str(e)}), 500

    elif request.method == 'GET':
        try:
            conn = db_connection()
            cursor = conn.cursor(dictionary=True)

            #This query is to assign the topic to the sensor_id
            cursor.execute("""
                SELECT timestamp, location_id, sensor_id, topic_value,
                CASE 
                    WHEN sensor_id = 1 THEN 'temperature'
                    WHEN sensor_id = 2 THEN 'humidity'
                    WHEN sensor_id = 3 THEN 'air_quality'
                    ELSE 'unknown'
                END AS topic
                FROM fact_observations
                ORDER BY timestamp
            """)
            rows = cursor.fetchall()

            cursor.close()
            conn.close()

            logging.info(f"Data retrieved successfully: {len(rows)} records")
            return jsonify(rows), 200

        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")
            return jsonify({"error": str(err)}), 500
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)

