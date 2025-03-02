from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
import psutil
import time
import requests
from bs4 import BeautifulSoup
from threading import Thread

# Initialize the Flask app and Flask-SocketIO
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)  # Remove async_mode='threading'

# Function to fetch sublinks from the given URL
def get_sublinks(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Find all the anchor tags (<a>) and extract the href attributes
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('http'):
                links.append(href)
            else:
                links.append(url + href)
        return links
    except Exception as e:
        print("Error fetching sublinks:", e)
        return []

# Function to log system usage (CPU and memory) every second
def log_system_usage():
    while True:
        # Fetch CPU and memory usage
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_percent = psutil.virtual_memory().percent
        
        # Emit the data to the frontend using SocketIO
        socketio.emit('system_usage', {'cpu': cpu_percent, 'memory': memory_percent})
        
        # Sleep for 1 second before the next check
        time.sleep(1)

# Function to start the system usage logging in a separate thread
def start_logging():
    system_usage_thread = Thread(target=log_system_usage)
    system_usage_thread.daemon = True
    system_usage_thread.start()

# Initialize the logging of system usage
start_logging()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_sublinks')
def fetch_sublinks():
    url = request.args.get('url')
    sublinks = get_sublinks(url)
    return jsonify({'sublinks': sublinks})

@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

if __name__ == '__main__':
    # Run the Flask app with SocketIO
    socketio.run(app, debug=True)
