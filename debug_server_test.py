import os
from dotenv import load_dotenv
import urllib.request
import json

# Load environment variables exactly like the server does
load_dotenv()

# Get the key exactly like the server does
GEMINI_API_KEY = os.getenv("gemini_api_key") or os.getenv("GEMINI_API_KEY")

print(f"Key loaded: '{GEMINI_API_KEY}'")
print(f"Key length: {len(GEMINI_API_KEY) if GEMINI_API_KEY else 'None'}")
print(f"Key starts with 'AIza': {GEMINI_API_KEY.startswith('AIza') if GEMINI_API_KEY else False}")

# Test the exact same request the server makes
if GEMINI_API_KEY:
    model = "gemini-2.0-flash"
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
    
    print(f"\nTesting endpoint: {endpoint[:100]}...")
    
    test_body = {
        "contents": [{"role": "user", "parts": [{"text": "Generate a simple sentence with the German word 'sprechen'"}]}],
        "generationConfig": {"temperature": 0.1, "response_mime_type": "application/json"}
    }
    
    try:
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(test_body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            print("✅ SUCCESS! API key works!")
            print(f"Response: {result}")
            
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"❌ HTTP Error: {e.code} {e.reason}")
        print(f"Details: {error_body}")
        
        # Check if the key in the URL matches what we expect
        if GEMINI_API_KEY in endpoint:
            print("✅ Key is correctly placed in URL")
        else:
            print("❌ Key is not in URL correctly")
            
    except Exception as e:
        print(f"❌ Other error: {str(e)}")
else:
    print("❌ No API key found!")