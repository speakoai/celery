#!/usr/bin/env python3
"""
Script to regenerate avatar tags using OpenAI Vision API
"""

import os
import json
import requests
from openai import OpenAI
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def analyze_avatar_image(image_url, avatar_id):
    """
    Analyze avatar image using OpenAI Vision API and generate tags
    """
    # Initialize OpenAI client
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    
    # System prompt for avatar analysis
    system_prompt = """You are an expert at analyzing avatar images for a professional avatar catalog. 
    Analyze the provided image and generate descriptive tags that would help users find this avatar.
    
    Focus on these categories:
    - Gender (male, female, non-binary, unknown)
    - Age group (young, middle-aged, senior)
    - Ethnicity/Race (caucasian, black, asian, hispanic, indian, etc. - use "unknown" if unclear)
    - Occupation (businessman, teacher, student, artist, etc. - use "unknown" if unclear)
    - Expression (smiling, serious, neutral, friendly, confident, etc.)
    - Clothing (shirt, jacket, suit, dress, t-shirt, tank top, etc.)
    - Accessories (glasses, hat, earrings, necklace, etc.)
    - Hair (short, long, curly, blonde, brunette, black, red, etc.)
    - Style descriptors (professional, casual, formal, cartoon, etc.)
    - Other notable features
    
    Return ONLY a JSON array of descriptive tags as strings. Keep tags concise and relevant.
    Avoid redundant tags. Maximum 15 tags total.
    
    Example format: ["male", "young", "smiling", "green shirt", "short hair", "cartoon"]"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",  # Use GPT-4 with vision
            messages=[
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Analyze this avatar image (ID: {avatar_id}) and generate appropriate tags:"
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_url,
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            max_tokens=300,
            temperature=0.1
        )
        
        # Extract the response content
        content = response.choices[0].message.content.strip()
        
        # Parse the JSON array from the response
        try:
            tags = json.loads(content)
            if isinstance(tags, list):
                return tags
            else:
                print(f"Warning: Response was not a list. Got: {content}")
                return []
        except json.JSONDecodeError:
            print(f"Warning: Could not parse JSON from response: {content}")
            # Try to extract tags from a more verbose response
            if "[" in content and "]" in content:
                start = content.find("[")
                end = content.rfind("]") + 1
                json_part = content[start:end]
                try:
                    tags = json.loads(json_part)
                    return tags if isinstance(tags, list) else []
                except json.JSONDecodeError:
                    pass
            return []
            
    except Exception as e:
        print(f"Error analyzing image: {str(e)}")
        return []

def regenerate_avatar_tags(avatar_id="avatar_101"):
    """
    Regenerate tags for a specific avatar
    """
    # Current avatar data
    current_avatar = {
        "id": "avatar_101",
        "url": "https://assets.speako.ai/staff-profiles/avatar/greenshirt_cartoon_20251008_173043.webp",
        "tags": [
            "unknown",
            "young",
            "smiling",
            "green shirt",
            "short",
            "curly",
            "short curly hair"
        ]
    }
    
    print(f"Analyzing avatar: {avatar_id}")
    print(f"Image URL: {current_avatar['url']}")
    print(f"Current tags: {current_avatar['tags']}")
    print("\nAnalyzing image with OpenAI Vision API...")
    
    # Analyze the image
    new_tags = analyze_avatar_image(current_avatar['url'], avatar_id)
    
    if new_tags:
        # Create updated avatar object
        updated_avatar = {
            "id": avatar_id,
            "url": current_avatar['url'],
            "tags": new_tags
        }
        
        print(f"\n{'='*60}")
        print("ANALYSIS COMPLETE!")
        print(f"{'='*60}")
        print(f"Generated {len(new_tags)} new tags:")
        print(f"New tags: {new_tags}")
        
        print(f"\n{'='*60}")
        print("UPDATED AVATAR OBJECT:")
        print(f"{'='*60}")
        print(json.dumps(updated_avatar, indent=2))
        
        print(f"\n{'='*60}")
        print("COMPARISON:")
        print(f"{'='*60}")
        print(f"Old tags ({len(current_avatar['tags'])}): {current_avatar['tags']}")
        print(f"New tags ({len(new_tags)}): {new_tags}")
        
        # Show changes
        old_set = set(current_avatar['tags'])
        new_set = set(new_tags)
        
        added = new_set - old_set
        removed = old_set - new_set
        kept = old_set & new_set
        
        if added:
            print(f"\nAdded tags: {list(added)}")
        if removed:
            print(f"Removed tags: {list(removed)}")
        if kept:
            print(f"Kept tags: {list(kept)}")
            
        return updated_avatar
    else:
        print("❌ Failed to generate new tags")
        return None

if __name__ == "__main__":
    # Check for OpenAI API key
    if not os.getenv('OPENAI_API_KEY'):
        print("❌ Error: OPENAI_API_KEY environment variable not set")
        print("Please set your OpenAI API key:")
        print("export OPENAI_API_KEY='your-api-key-here'")
        exit(1)
    
    # Regenerate tags for avatar_101
    updated_avatar = regenerate_avatar_tags("avatar_101")
    
    if updated_avatar:
        print(f"\n{'='*60}")
        print("✅ SUCCESS!")
        print("Copy the 'UPDATED AVATAR OBJECT' above and paste it into your JSON file.")
        print(f"{'='*60}")
    else:
        print("\n❌ Failed to regenerate avatar tags")
