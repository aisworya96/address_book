from flask import Flask, request, jsonify
from geopy.distance import geodesic
import sqlite3

app = Flask(__name__)
DATABASE = 'addresses.db'

def init_db():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            latitude REAL,
            longitude REAL
        )
    ''')
    conn.commit()
    conn.close()

@app.route('/address', methods=['POST'])
def create_address():
    data = request.json
    address = data.get('address')
    latitude = data.get('latitude')
    longitude = data.get('longitude')

    if not (address and latitude and longitude):
        return jsonify({'error': 'Incomplete data provided'}), 400

    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('INSERT INTO addresses (address, latitude, longitude) VALUES (?, ?, ?)', (address, latitude, longitude))
        conn.commit()
        conn.close()
        return jsonify({'message': 'Address created successfully'}), 201
    except sqlite3.Error as e:
        return jsonify({'error': str(e)}), 500

@app.route('/address/<int:address_id>', methods=['PUT'])
def update_address(address_id):
    data = request.json
    address = data.get('address')
    latitude = data.get('latitude')
    longitude = data.get('longitude')

    if not (address and latitude and longitude):
        return jsonify({'error': 'Incomplete data provided'}), 400

    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('UPDATE addresses SET address=?, latitude=?, longitude=? WHERE id=?', (address, latitude, longitude, address_id))
        conn.commit()
        conn.close()
        return jsonify({'message': f'Address with id {address_id} updated successfully'}), 200
    except sqlite3.Error as e:
        return jsonify({'error': str(e)}), 500

@app.route('/address/<int:address_id>', methods=['DELETE'])
def delete_address(address_id):
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('DELETE FROM addresses WHERE id=?', (address_id,))
        conn.commit()
        conn.close()
        return jsonify({'message': f'Address with id {address_id} deleted successfully'}), 200
    except sqlite3.Error as e:
        return jsonify({'error': str(e)}), 500

@app.route('/address', methods=['GET'])
def get_addresses():
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('SELECT * FROM addresses')
        addresses = [{'id': row[0], 'address': row[1], 'latitude': row[2], 'longitude': row[3]} for row in c.fetchall()]
        conn.close()
        return jsonify(addresses), 200
    except sqlite3.Error as e:
        return jsonify({'error': str(e)}), 500

@app.route('/address/nearby', methods=['GET'])
def get_addresses_within_distance():
    try:
        data = request.args
        latitude = float(data.get('latitude'))
        longitude = float(data.get('longitude'))
        distance = float(data.get('distance'))

        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('SELECT * FROM addresses WHERE (latitude - ?) * (latitude - ?) + (longitude - ?) * (longitude - ?) <= ? * ?',
                  (latitude, latitude, longitude, longitude, distance, distance))
        nearby_addresses = [{'id': row[0], 'address': row[1], 'latitude': row[2], 'longitude': row[3]} for row in c.fetchall()]
        conn.close()
        return jsonify(nearby_addresses), 200
    except sqlite3.Error as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    init_db()
    app.run(debug=True)

