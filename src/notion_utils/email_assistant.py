import os
import sys
import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import base64

import subprocess
import json

def safe_notify(title, message, subtitle="", sound=None):
    """Safe notification with proper escaping"""
    
    # Escape special characters
    title = title.replace('"', '\\"').replace("'", "\\'")
    message = message.replace('"', '\\"').replace("'", "\\'")
    
    script_parts = [
        'display notification',
        f'"{message}"',
        'with title',
        f'"{title}"'
    ]
    
    if subtitle:
        subtitle = subtitle.replace('"', '\\"').replace("'", "\\'")
        script_parts.extend(['subtitle', f'"{subtitle}"'])
    
    if sound:
        script_parts.extend(['sound name', f'"{sound}"'])
    
    script = ' '.join(script_parts)
    
    try:
        subprocess.run(
            ["osascript", "-e", script],
            check=True,
            capture_output=True,
            text=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Notification failed: {e}")
        return False


def make_api_request_with_retry(api_key, prompt_text, max_retries=3):
    """Make API request with retry logic for handling rate limits and server errors."""

    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504, 529],  # Retry on these HTTP status codes
        backoff_factor=1,  # Wait 1, 2, 4, 8... seconds between retries
        allowed_methods=["POST"]  # Only retry POST requests
    )

    # Create session with retry adapter
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    data = {
        'model': 'claude-3-5-sonnet-20241022',
        'max_tokens': 1000,
        'temperature': 0.7,
        'messages': [{'role': 'user', 'content': prompt_text}]
    }

    headers = {
        'x-api-key': api_key,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
    }

    for attempt in range(max_retries + 1):
        try:
            response = session.post(
                'https://api.anthropic.com/v1/messages',
                json=data,
                headers=headers,
                timeout=30  # 30 second timeout
            )
            response.raise_for_status()  # Raise exception for bad status codes
            return response.json()

        except requests.exceptions.HTTPError as e:
            if response.status_code == 529:
                # Service overloaded - use exponential backoff
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * 2  # 2, 4, 8 seconds
                    print(f"Service overloaded (HTTP 529). Retrying in {wait_time} seconds...", file=sys.stderr)
                    time.sleep(wait_time)
                    continue
            elif response.status_code == 429:
                # Rate limited - check for Retry-After header
                retry_after = response.headers.get('retry-after')
                if retry_after and attempt < max_retries:
                    wait_time = int(retry_after) if retry_after.isdigit() else 60
                    print(f"Rate limited. Waiting {wait_time} seconds...", file=sys.stderr)
                    time.sleep(wait_time)
                    continue
            raise e
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = (2 ** attempt)
                print(f"Request failed: {e}. Retrying in {wait_time} seconds...", file=sys.stderr)
                time.sleep(wait_time)
                continue
            raise e

    raise Exception(f"Failed after {max_retries + 1} attempts")



prompt = '''
You are an expert email assistant for Kasper, a professor at Aarhus University. 
Your task is to draft concise, context-aware email responses based on the provided 
email content and sender information. You will also identify any actionable tasks 
mentioned in the email and format them for easy addition to Kasper's Notion task 
database.

IMPORTANT: UNDER NO CIRCUMSTANCES SHOULD YOU RETURN ANYTHING BUT VALID JSON.

The JSON object must contain two fields:
1. "email_draft": The email response text if a response is needed, otherwise an empty string
2. "tasks": An array of task objects (only if tasks can be identified in email)

Each task object must have:
- "title": The title of the task
- "due_date": Format as "YYYY-MM-DD" (e.g., "2025-09-15")
- "note": Any information aiding completion of the task.



Example response format:
{{
  "email_draft": "Your email text here... Can be empty if no response needed.",
  "tasks": [
    {{"title": "Send input to Christian", "due_date": "2025-09-20", "note": ""}},
    {{"title": "Find paper for Christian's student", "due_date": "", "note": "Look for a paper on XYZ topic"}}
    ]
}}

IMPORTANT:
- Return ONLY valid JSON
- If no response is needed, return an empty string for email_draft
- If no meetings are mentioned, return an empty array for suggested_meetings
- If tasks can be identified, add a "tasks" field with an array of todo objects ("title" and optional "due_date")
- If no tasks, return an empty array for tasks
- In the email_draft, write in the same language as the original email
- If meeting slots are provided, select 2-3 most appropriate ones
- Convert dates like "Monday Sep 16" to "2025-09-16" format
- Convert times like "10:00 AM" to "10:00" and "2:00 PM" to "14:00"
- Keep professional but warm tone
- Sign with: Kasper Munch (or just Kasper for informal)
- Use appropriate closing for the language

INSTRUCTIONS: "{cleanInstructions}"

ORIGINAL EMAIL:
From: "{cleanSender}"
Subject: "{cleanSubject}"
Content: "{emailSnippet}"

CORE VOICE CHARACTERISTICS

- **Brevity is key**: Keep responses concise and to the point. Most emails are 2-5 sentences.
- **Direct and friendly**: Use a warm but professional tone without excessive formalities
- **Context-dependent formality**: Adjust based on sender's domain and relationship

RESPONSE PATTERNS BY CONTEXT

For @au.dk (Internal/Colleagues):

- **Very brief**: Often 1-3 lines maximum
- **Casual Danish or English**: "Hej" for Danish speakers, informal tone
- **Minimal explanation**: Assume shared context
- Examples:
  - "Hej [Name], Jeg ved ikke lige hvordan jeg skal finde dit hold. Hvilken uddannelse er du på? VH Kasper"
  - Meeting acceptances: Simple "Accepted: [Meeting title]" with no body text

For @post.au.dk (Students):

- **Danish students**: Respond in Danish, use "Hej" and "VH Kasper"
- **Brief instructions**: "Prøv at følge installationsproceduren som den er beskrevet på hjemme siden nu. Det skulle virke nu."
- **No over-explaining**: Direct them to resources rather than elaborate
- **Scheduling**: For scheduling a meeting, ask for 2-3 time suggestions if not specified.

For External/International:

- **Slightly more formal**: But still concise
- **Apologetic when delayed**: "Just dug you out of the giant email inbox I have accumulated over the summer... Sorry about not getting back to you sooner."
- **Clear next steps**: "The next week or so is a bit crazy for me. We have job interviews Thurs, Friday..."
- **Sign-off**: "Cheers, Kasper" for familiar contacts, "Best regards" for formal

For Rejections/Declining:

- **Polite but firm**: "Sorry to say I only have time to supervise projects in the master in bioinformatics"
- **Brief reasoning**: Don't elaborate on why
- **No false hope**: Clear and definitive

Special Patterns:

- **Use emoji sparingly**: Only in very casual contexts with familiar colleagues ("I would be happy to Zoom and catch up 😊")
- **Calendar responses**: Minimal or no text, just accept/decline
- **Time-sensitive**: Acknowledge if you're late responding
- **Mobile signature**: "Sent from Outlook for iOS" when applicable

TEMPLATE STRUCTURE

```
[Greeting - Hej/Hi/Dear based on context],

[1-2 sentences addressing the main point directly]
[If needed, one sentence about next steps or timing]

[Sign-off - VH Kasper/Cheers/Best regards based on formality]
```

KEY RULES

1. **Never use more words than necessary** - If it can be said in 2 sentences, don't use 3
2. **Match the language** (Danish/English) of the sender when from @au.dk
3. **Be helpful but set clear boundaries** - Direct but not harsh
4. **Don't explain what doesn't need explaining** - Assume competence

EXAMPLE RESPONSES BY SCENARIO

Student Technical Issue (Danish):

Hej [Name],
Prøv at følge installationsproceduren som den er beskrevet på hjemme siden nu. Det skulle virke nu.
VH Kasper


Declining Review Request:

Hi Jeff,
Really sorry about dropping this one. Got buried in all the email accumulating over the summer.
Kasper


Scheduling with External Collaborator:

Hi [Name],
I can meet 11:30 or 14:00 tomorrow. Does that work for you?
Kasper

Rejecting PhD Application:

Dear [Name],
Sorry to say I only have time to supervise projects in the master in bioinformatics.
Best
Kasper

DOMAIN-SPECIFIC NOTES

- **@birc.au.dk**: Extremely brief, assume full context
- **@post.au.dk**: Student emails - helpful but brief, often in Danish
- **@gmail.com/@outlook.com**: More formal, full sentences, proper greeting/closing
- **International academic (.edu, .fr, etc.)**: Professional but friendly, acknowledge delays if applicable
'''


def handle_email():

    json_file = sys.argv[1]

    with open(json_file, 'r', encoding='utf-8') as f:
        input_data = json.load(f)

    try:
        api_key = input_data['apiKey']
        prompt_text = prompt.format(**input_data)

        # Make API request with retry logic
        result = make_api_request_with_retry(api_key, prompt_text)
        claude_response = result['content'][0]['text']

        response_data = json.loads(claude_response)


        from .notion_page import NotionPageCreator
        creator = NotionPageCreator('NOTION_API_KEY') ############ GET THE KEY FROM KEYCHAIN IN APPLESCRIPT AND PASS IT TO THIS SCRIPT
        for i, task in enumerate(response_data.get('tasks', [])):
            title = task['title']
            due_date = task.get('due_date', '')
            note = task.get('note', '')
            content = f"Due date: {due_date}\n\n{note}" if due_date or note else ''
            # Here you would add the code to create a Notion page using your NotionPageCreator
            # For example:
            creator.create_page('25ffd1e7c2e1800d9be9f8b38365b1c6', title, content)
            safe_notify(f"Task {i+1}: " + title, content, sound="Ping")
    
        # Convert to JSON
        json_string = json.dumps(response_data, ensure_ascii=False)
        
        # Base64 encode
        encoded = base64.b64encode(json_string.encode('utf-8')).decode('ascii')
        
        # Return only the base64 string to applescript via stdout
        print(encoded)


    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f'ERROR: Invalid API key', file=sys.stderr)
        elif e.response.status_code == 429:
            print(f'ERROR: Rate limit exceeded. Please try again later.', file=sys.stderr)
        elif e.response.status_code == 529:
            print(f'ERROR: Service temporarily unavailable. Please try again later.', file=sys.stderr)
        else:
            print(f'ERROR: HTTP {e.response.status_code}: {e}', file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        print('ERROR: Unable to connect to Anthropic API. Check your internet connection.', file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.Timeout:
        print('ERROR: Request timed out. Please try again.', file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f'ERROR: Invalid JSON in response: {e}', file=sys.stderr)
        sys.exit(1)
    except KeyError as e:
        print(f'ERROR: Missing required field in input: {e}', file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        raise e
        # print(f'ERROR: {e}', file=sys.stderr)
        # sys.exit(1)
