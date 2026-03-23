from google import genai
import os

api_key = os.environ.get("GEMINI_API_KEY")

client = genai.Client(api_key=api_key)

print("Available models:\n")

for m in client.models.list():
    print(m.name)