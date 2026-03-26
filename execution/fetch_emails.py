import os
import base64
from datetime import datetime
from dotenv import load_dotenv
import requests

load_dotenv()

GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

def get_access_token():
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    
    return response.json()["access_token"]

def fetch_unread_emails_with_attachments():
    access_token = get_access_token()
    
    sender_filter = (
        "from/emailAddress/address eq 'd.oliveira@custom.biz' or "
        "from/emailAddress/address eq 's.oliveira@custom.biz' or "
        "from/emailAddress/address eq 'edulechuga@gmail.com'"
    )
    
    filter_query = f"{sender_filter} and hasAttachments eq true"
    
    url = f"{GRAPH_API_URL}/me/messages"
    params = {
        "$filter": filter_query,
        "$select": "id,subject,from,hasAttachments,receivedDateTime",
        "$top": 50
    }
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    messages = response.json().get("value", [])
    emails = []
    
    for msg in messages:
        attachments = get_attachments(msg["id"])
        
        emails.append({
            "id": msg["id"],
            "subject": msg.get("subject"),
            "from": msg["from"]["emailAddress"]["address"],
            "received_date": msg.get("receivedDateTime"),
            "attachments": attachments
        })
        
        mark_as_read(msg["id"])
    
    return emails

def get_attachments(message_id):
    access_token = get_access_token()
    
    url = f"{GRAPH_API_URL}/me/messages/{message_id}/attachments"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    attachments = []
    for att in response.json().get("value", []):
        if att.get("@odata.type") == "#microsoft.graph.fileAttachment":
            attachments.append({
                "filename": att.get("name"),
                "mime_type": att.get("contentType"),
                "content": base64.b64decode(att.get("contentBytes")))
            })
    
    return attachments

def mark_as_read(message_id):
    access_token = get_access_token()
    
    url = f"{GRAPH_API_URL}/me/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    requests.patch(url, headers=headers, json={"isRead": True})