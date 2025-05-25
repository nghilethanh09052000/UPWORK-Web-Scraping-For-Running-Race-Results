import requests
import base64
import json

# Parameters
event_id = 69430
term = 10000

# URL
url = f"https://sportstimingsolutions.in/frontend/api/event-bibs?event_id={event_id}&term={term}"

# Headers (taken from your request)
headers = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
    "Referer": "https://sportstimingsolutions.in/results?q=eyJlX25hbWUiOiJMYWRha2ggTWFyYXRob24gMjAyMiIsImVfaWQiOjY5NDMwfQ%3D%3D",
}

# Optional: If the request fails without cookies or XSRF token, add them here
cookies = {
    # Add values if needed. Try without first.
}

# Send request
response = requests.get(url, headers=headers, cookies=cookies)

# Parse base64-encoded JSON from response
encoded_data = response.json().get("data")
decoded_bytes = base64.b64decode(encoded_data)
decoded_json = json.loads(decoded_bytes)

# Print each participant
for p in decoded_json.get("participants", []):
    print(f"{p['bibno']} - {p['full_name']}")
