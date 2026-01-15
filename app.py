from flask import Flask, render_template, request, jsonify, send_file # type: ignore
import subprocess
import os
import json
import time
import threading
from datetime import datetime
import csv

app = Flask(__name__)

# Status tracking
scraper_status = {
    "running": False,
    "progress": 0,
    "current_business": "",
    "total_businesses": 0,
    "message": "Ready",
    "results": None,
    "error": None
}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    return jsonify(scraper_status)

@app.route('/api/scrape', methods=['POST'])
def start_scrape():
    global scraper_status
    
    if scraper_status["running"]:
        return jsonify({"error": "Scraper is already running"}), 400
    
    data = request.json
    keyword = data.get('keyword', '').strip()
    city = data.get('city', '').strip()
    max_results = data.get('max_results')
    no_emails = data.get('no_emails', False)
    headless = data.get('headless', True)
    timeout = data.get('timeout', 300)  # Default 5 minutes
    
    if not keyword or not city:
        return jsonify({"error": "Keyword and city are required"}), 400
    
    # Reset status
    scraper_status = {
        "running": True,
        "progress": 0,
        "current_business": "",
        "total_businesses": 0,
        "message": "Starting scraper...",
        "results": None,
        "error": None
    }
    
    # Start scraper in background thread
    thread = threading.Thread(
        target=run_scraper,
        args=(keyword, city, max_results, no_emails, headless, timeout)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({"status": "started"})

def run_scraper(keyword, city, max_results, no_emails, headless, timeout):
    global scraper_status
    
    try:
        # Build command
        cmd = [
            "python",
            "maps_scraper.py",
            "--keyword", keyword,
            "--city", city,
            "--verbose"
        ]
        
        # headless should be True by default (headless=False when headless is passed as False)
        if not headless:
            # Only add --headless if we want headless mode
            pass
        else:
            cmd.append("--headless")
        
        cmd.extend(["--timeout", str(timeout)])
        
        if max_results:
            cmd.extend(["--max-results", str(max_results)])
        
        if no_emails:
            cmd.append("--no-emails")
        
        scraper_status["message"] = "🚀 Launching browser..."
        scraper_status["progress"] = 5
        
        # Run scraper with timeout
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            timeout=timeout + 30  # Add 30s buffer
        )
        
        scraper_status["progress"] = 75
        scraper_status["message"] = "📊 Processing results..."
        
        if result.returncode != 0:
            scraper_status["error"] = result.stderr or "Scraper failed"
            scraper_status["running"] = False
            return
        
        # Load results
        csv_file = os.path.join("output", "businesses.csv")
        json_file = os.path.join("output", "businesses.json")
        
        results = []
        if os.path.exists(csv_file):
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                results = list(reader)
        
        scraper_status["progress"] = 100
        scraper_status["message"] = f"✅ Complete! Found {len(results)} businesses"
        scraper_status["results"] = results
        scraper_status["total_businesses"] = len(results)
        scraper_status["running"] = False
        
    except Exception as e:
        scraper_status["error"] = str(e)
        scraper_status["running"] = False

@app.route('/api/results/csv')
def download_csv():
    csv_file = os.path.join("output", "businesses.csv")
    if not os.path.exists(csv_file):
        return jsonify({"error": "No results available"}), 404
    
    return send_file(
        csv_file,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f"businesses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

@app.route('/api/results/json')
def download_json():
    json_file = os.path.join("output", "businesses.json")
    if not os.path.exists(json_file):
        return jsonify({"error": "No results available"}), 404
    
    return send_file(
        json_file,
        mimetype='application/json',
        as_attachment=True,
        download_name=f"businesses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

if __name__ == '__main__':
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("checkpoints", exist_ok=True)
    print("🚀 Starting UI Server...")
    print("📱 Open http://localhost:5000 in your browser")
    app.run(debug=True, port=5000)
