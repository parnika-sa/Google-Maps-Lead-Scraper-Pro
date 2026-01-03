# 📍 Google Maps Lead Scraper Pro

**Extract Business Leads from Google Maps Automatically** 🚀

A professional automation tool to scrape business data (name, phone, website, emails) from Google Maps with a beautiful web interface.

![Python](https://img.shields.io/badge/Python-3.8+-blue)
![Flask](https://img.shields.io/badge/Flask-3.0+-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 🎯 **What This Does (Ek Sentence Mein)**

> **Problem:** Manual data collection = time-consuming, error-prone  
> **Solution:** This tool automatically extracts 100+ business leads from Google Maps in minutes

---

## ⚡ **Use Cases**

✅ **Business Development** - Find prospects automatically  
✅ **Sales Lead Generation** - Build targeted contact lists  
✅ **Market Research** - Analyze competitor presence  
✅ **Real Estate** - Find property-related businesses  
✅ **Franchise Research** - Compare business density by area  

---

## 🚀 **Quick Start (3 Steps)**

### **Step 1: Install**
```bash
pip install -r requirements.txt
playwright install chromium
```

### **Step 2: Run**
```bash
python app.py
```

### **Step 3: Open Browser**
```
http://localhost:5000
```

Then:
1. Enter business type (e.g., "Restaurant", "Plumber", "Cafe")
2. Enter city (e.g., "New York", "Mumbai", "London")
3. Click "Start Scraping"
4. Download CSV or JSON

**That's it!** ✅

---

## 📊 **What You Get**

| Field | Example |
|-------|---------|
| **Business Name** | Starbucks Coffee |
| **Address** | 123 Main St, New York, NY 10001 |
| **Phone** | (212) 555-0123 |
| **Website** | https://starbucks.com |
| **Emails** | manager@starbucks.com |

### **Sample Output (CSV)**
```
name,address,phone,website,emails
Starbucks,"123 Main St, New York","(212) 555-0123","https://starbucks.com","manager@starbucks.com"
Blue Bottle Coffee,"456 Park Ave, New York","(212) 555-0456","https://bluebottlecoffee.com","contact@bluebottlecoffee.com"
Joe Coffee Company,"789 Broadway, New York","(212) 555-0789","https://joecoffee.com","N/A"
```

### **Sample Output (JSON)**
```json
[
  {
    "name": "Starbucks",
    "address": "123 Main St, New York",
    "phone": "(212) 555-0123",
    "website": "https://starbucks.com",
    "emails": "manager@starbucks.com"
  }
]
```

---

## 🎮 **Usage Examples**

### **Example 1: Find All Pizza Places in NYC**
```
Keyword: Pizza
City: New York
Max Results: Leave empty (get all)
Timeout: 15 minutes
Skip Emails: OFF (get emails too)
Result: 150+ pizza restaurants with phone & email
```

### **Example 2: Quick Test (5 Restaurants)**
```
Keyword: Restaurant
City: Los Angeles
Max Results: 5
Timeout: 5 minutes
Skip Emails: ON (faster)
Result: Quick list in ~3 minutes
```

### **Example 3: B2B Lead Generation**
```
Keyword: Digital Marketing Agency
City: San Francisco
Max Results: 100
Timeout: 20 minutes
Skip Emails: OFF
Result: 100 agencies with contact info
```

---

## ⚙️ **Features**

### **Web Interface**
✅ No coding needed - just click and fill  
✅ Real-time progress tracking  
✅ Beautiful dashboard with stats  
✅ Mobile responsive  

### **Smart Scraping**
✅ Automatic retry on errors  
✅ Smart rate limiting (won't get banned)  
✅ Email validation (no false positives)  
✅ Duplicate removal  

### **Professional Output**
✅ CSV format (for Excel/Sheets)  
✅ JSON format (for APIs)  
✅ Detailed logs for debugging  
✅ Resume capability (don't lose progress)  

---

## 📋 **Settings Explained**

| Setting | What It Does |
|---------|-------------|
| **Keyword** | Type of business (e.g., "Cafe", "Gym", "Lawyer") |
| **City** | Location to search (e.g., "New York", "Mumbai") |
| **Max Results** | Stop after N businesses (leave empty for all) |
| **Timeout** | Max time to run (5-30 minutes) |
| **Skip Emails** | Faster if you don't need emails |
| **Headless Mode** | Faster (no visible browser) |

---

## 📈 **Performance**

| Scenario | Results | Time |
|----------|---------|------|
| Small city search | 30-50 | 3-5 min |
| Medium city | 100-150 | 10-15 min |
| Large city (no emails) | 200+ | 15-20 min |
| Large city (with emails) | 100-150 | 20-30 min |

---

## 🛠️ **How It Works**

1. **Search** → Opens Google Maps & searches your keyword
2. **Scroll** → Loads all available results (not just first page)
3. **Extract** → Gets name, phone, website, address
4. **Email Mine** → Visits websites & extracts emails (optional)
5. **Deduplicate** → Removes duplicates & validates data
6. **Export** → Saves as CSV & JSON

**See detailed workflow:** [HOW_IT_WORKS.md](HOW_IT_WORKS.md)

---

## 📁 **Output Files**

```
output/
├── businesses.csv      ← Open in Excel
└── businesses.json     ← Use in APIs

logs/
└── scraper_*.log       ← Debugging info
```

---

## 💻 **Command Line (Advanced)**

If you prefer terminal:

```bash
# Basic search
python maps_scraper.py --keyword "Restaurant" --city "New York"

# With limits
python maps_scraper.py --keyword "Cafe" --city "London" --max-results 50

# Fast mode (no emails)
python maps_scraper.py --keyword "Gym" --city "Paris" --no-emails --headless

# Resume from checkpoint
python maps_scraper.py --keyword "Plumber" --city "Mumbai" --resume --timeout 900
```

---

## ⚡ **Tips for Best Results**

1. **Start with 10 results** → Test before big scrape
2. **Use headless mode** → 2-3x faster (default: ON)
3. **Skip emails first** → Get all businesses, extract emails later
4. **Specific keywords work better** → "Italian Restaurant" > "Restaurant"
5. **Run at off-hours** → Less chance of getting blocked

---

## ⚠️ **Important**

- ✅ Legal for personal/business use
- ✅ Respects rate limits (built-in delays)
- ⚠️ Don't scrape too aggressively (may get IP blocked)
- 💡 Consider proxies for large-scale scraping

---

## 🐛 **Troubleshooting**

| Problem | Solution |
|---------|----------|
| No results found | Try simpler keyword or different city |
| Very slow | Enable "Skip Emails", reduce timeout |
| Getting blocked | Wait 2-3 hours, try different city |
| Crashes midway | Use "Resume" feature to continue |

See [HOW_IT_WORKS.md](HOW_IT_WORKS.md) for complete guide.

---

## 📦 **Requirements**

- Python 3.8+
- Flask 3.0+
- Playwright (Chromium browser)

---

## 🚀 **Installation (Detailed)**

### **1. Clone Repository**
```bash
git clone https://github.com/parnika-sa/maps.git
cd maps
```

### **2. Create Virtual Environment**
```bash
python -m venv venv
venv\Scripts\activate
```

### **3. Install Dependencies**
```bash
pip install -r requirements.txt
playwright install chromium
```

### **4. Run**
```bash
python app.py
```

### **5. Open Browser**
```
http://localhost:5000
```

Done! 🎉

---

## 📝 **License**

MIT License - Free to use for personal & commercial projects

---

## 🤝 **Contributing**

Got ideas to improve? Send pull request!

---

## 📞 **Support**

Questions? Check [HOW_IT_WORKS.md](HOW_IT_WORKS.md) for detailed documentation.

---

## ⭐ **If This Helps, Star It!**

Your support motivates me to improve this tool 🙏

---

**Made with ❤️ by Ankit Maurya**
