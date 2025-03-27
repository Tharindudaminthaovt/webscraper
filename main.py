from flask import Flask, jsonify
import pandas as pd
from flask_cors import CORS
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait
import datetime
import pytz
from flask_socketio import SocketIO, emit
import time
import threading
from firebase_admin import credentials, initialize_app, db
import firebase_admin

# Initialize Flask and Socket.IO
app = Flask(__name__)

socketio = SocketIO(app)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize Firebase
cred = credentials.Certificate("creds.json")  # Ensure this path is correct
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://stock-market-data-8947e-default-rtdb.asia-southeast1.firebasedatabase.app/'
})

# Global Firebase reference
ref = db.reference('/cse_data')

def scrape_cse_data():
    """Scrape data from CSE website."""
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    today = utc_now.astimezone(pytz.timezone("Asia/Colombo")).strftime('%Y-%m-%d')
      # 
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)

    driver.get('https://www.cse.lk/pages/trade-summary/trade-summary.component.html')
    final_select = Select(driver.find_element("name", 'DataTables_Table_0_length'))
    final_select.select_by_visible_text('All')

    WebDriverWait(driver, 3)
    df = pd.read_html(driver.page_source)[0]
    df['Date'] = today


# Check the column names
   

# Sanitize column names
    df.columns = df.columns.str.replace(r'[\$#\[\]\/\.\s]', '_', regex=True)
    print(df.columns)
    driver.quit()
    return df.to_dict(orient='records')

def background_scraper():
    """Continuous scraping and broadcasting."""
    while True:
        try:
            utc_now = pytz.utc.localize(datetime.datetime.utcnow())
            today = utc_now.astimezone(pytz.timezone("Asia/Colombo")).strftime('%Y-%m-%d')

            # Check if today's data already exists
            #existing_data = ref.order_by_child('Date').equal_to(today).get()
            existing_keys = ref.get()
            if existing_keys:
                for key in existing_keys.keys():
                    if key.startswith(today):  # Check if any key starts with today's date
                        print(f"Data for {today} already exists in Firebase (Key: {key}). Skipping scrape.")
                        time.sleep(8640)  # Sleep for 24 hours before checking again
                        continue# Skip to the next iteration

            data = scrape_cse_data()
            if not data:
                print("No data scraped.")
                time.sleep(300)  # Sleep for 5 minutes before retrying
                continue  # Skip to the next iteration

            timestamp = datetime.datetime.now().isoformat()
            sanitized_timestamp = timestamp.replace('.', '_')

            # Store data in Firebase
            ref.child(sanitized_timestamp).set(data)

            # Send to WebSocket clients
            socketio.emit('update', {'data': data, 'timestamp': timestamp})

            print(f"Data pushed to Firebase with timestamp: {timestamp}")

        except Exception as e:
            print(f"Scraping error: {str(e)}")

        time.sleep(300)  # Sleep for 5 minutes before next attempt


@app.route('/get_cse_data', methods=['GET'])
def get_cse_data():
    """Endpoint for manual data retrieval."""
    data = scrape_cse_data()
    return jsonify(data)

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connections."""
    print('Client connected')
    emit('status', {'message': 'Connected to live CSE feed'})

if __name__ == '__main__':
    # Start background scraper thread
    threading.Thread(target=background_scraper, daemon=True).start()

    # Run Socket.IO app
    socketio.run(app, host='0.0.0.0', port=8080, debug=True)