import re
import requests # used to send HTTP requests and recieve responses from web servers
from pymongo import MongoClient
import time

SUBREDDIT_URL = "https://www.reddit.com/r/changemyview/.json"
FILTER_PHRASES = [r"\bisrael\b", r"\bjew\b"]

# MongoDB setup
MONGO_URI = (
    "mongodb+srv://ostulbach:XfEjXMll5j4egCwD@redditscraper.ynswvec.mongodb.net"
    "/?retryWrites=true&w=majority&appName=RedditScraper"
)
client = MongoClient(MONGO_URI)
collection = client.reddit_scraper.israel_posts

headers = {
    "User-Agent": "script:keyword-extractor:v1.0 (by /u/Helpful-Teacher1105)"
}

# Reddit represents every comment (and its replies) as nested dictionaries, which together form a tree. This function performs a DFS through that tree, finding and returning only the comments whose body text matches one of your keyword patterns.
def collect_matching_comments(children, patterns): 
    # children = list of child objects at current level of tree, of format "{"kind": "t1", "data": {...}}"
    # patterns = list of compiled or regex strings
    matched = [] # dictionaries for every comment that passes the filter
    for child in children:
        if child["kind"] != "t1": #t1 = regular comment
            continue
        cdata = child["data"] # holds comment metadata
        body = cdata.get("body", "") # body of comment, or default empty string
        if any(re.search(p, body, re.I) for p in patterns): # if any pattern matches, comment is added to
            matched.append({
                "id": cdata["id"],
                "author": cdata.get("author"),
                "score": cdata.get("score"),
                "created_utc": cdata.get("created_utc"),
                "body": body,
            })
        replies = cdata.get("replies")
        if replies and isinstance(replies, dict):
            matched.extend(collect_matching_comments(replies["data"]["children"], patterns)) # recursively search replies, adding relevant replies to matched dictionary
    return matched

def get_with_retry(url, headers, max_retries=5, backoff=10):
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 429:
            print(f"Rate limited on {url}, retrying in {backoff} seconds...")
            time.sleep(backoff)
            backoff *= 2  # exponential backoff (optional)
        else:
            resp.raise_for_status()
            return resp
    raise Exception(f"Failed to get {url} after {max_retries} retries due to rate limiting.")

# STEP 1: Load the list of posts from subreddit
resp = get_with_retry(SUBREDDIT_URL, headers=headers) # sends GET request to URL, identifies my script to Reddit, waits up to 10 seconds for Reddit to respond --> resp is a Response object that contains the server's reply
# resp.raise_for_status() # if the HTTP status code is not 200, it raises an error
posts_data = resp.json() # parses JSON content from response -- turns it into Python object

for child in posts_data["data"]["children"]:
    post = child["data"]
    permalink = post["permalink"]
    full_url = "https://www.reddit.com" + permalink
    post_id = post["id"]

    try:
        # STEP 2: Fetch full JSON for the post
        post_resp = get_with_retry(full_url + ".json", headers=headers)
        # post_resp.raise_for_status()
        payload = post_resp.json()

        submission = payload[0]["data"]["children"][0]["data"]
        title = submission["title"]
        selftext = submission["selftext"]
        keyword_in_title = any(re.search(p, title, re.I) for p in FILTER_PHRASES)
        keyword_in_selftxt = any(re.search(p, selftext, re.I) for p in FILTER_PHRASES)
        keyword_comments = collect_matching_comments(payload[1]["data"]["children"], FILTER_PHRASES)

        if keyword_in_title or keyword_in_selftxt or keyword_comments:
            collection.insert_one({
                "post_id": post_id,
                "url": full_url,
                "title": title,
                "content": selftext,
                "keyword_comments": keyword_comments
            })
            # print(f"✓ Inserted: {title[:60]}...")
        # else:
        #     print(f"Skipped: {title[:60]}...")
            
        time.sleep(2)

    except Exception as e:
        print(f"Failed to process {full_url}: {e}")

print("✅ Done scraping r/changemyview.")

import gspread
from google.oauth2.service_account import Credentials

# Load credentials
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# Open sheet
SHEET_NAME = "Sixpoint Media Reddit Scraper"
sheet = client.open(SHEET_NAME).worksheet("Posts")  # or .sheet1 if default

# Write headers (run once)
if sheet.acell('A1').value is None:
    sheet.append_row(["Title", "Content", "URL", "Matching Comment"])

# Write each post
rows_to_append = []

for result in collection.find():  # from MongoDB
    title = result.get("title", "")
    content = result.get("content", "")
    url = result.get("url", "")
    comments = result.get("keyword_comments", [])
    for comment in comments:
        rows_to_append.append([title, content, url, comment["body"]])

sheet.append_rows(rows_to_append)

print("✅ Done writing to Google Sheets.")
