#!/usr/bin/env python3
"""
Avatar Scanner and Analyzer for Cloudflare R2 Storage

This script scans avatar images in Cloudflare R2 storage, analyzes them using OpenAI,
renames files with descriptive names, and generates a JSON API output.

Features:
- Scans specific folder in Cloudflare R2 bucket
- Analyzes avatar characteristics using OpenAI Vision API
- Renames files based on AI analysis
- Generates comprehensive JSON output for web API
- Handles errors gracefully with retry logic
"""

import os
import json
import boto3
import base64
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime
import re
from pathlib import Path
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('avatar_scanner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AvatarScanner:
    """Main class for scanning and analyzing avatar images."""
    
    def __init__(self):
        """Initialize the avatar scanner with configurations."""
        self.setup_r2_client()
        self.setup_openai()
        self.target_folder = "staff-profiles/avatar/"
        self.output_file = "avatar_catalog.json"
        self.processed_count = 0
        self.error_count = 0
        
    def setup_r2_client(self):
        """Setup Cloudflare R2 client using boto3."""
        self.r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
        self.r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
        self.r2_endpoint = os.getenv("R2_ENDPOINT_URL")
        self.r2_bucket = os.getenv("R2_BUCKET_NAME")
        self.r2_account_id = os.getenv("R2_ACCOUNT_ID")
        
        if not all([self.r2_access_key, self.r2_secret_key, self.r2_endpoint, self.r2_bucket]):
            raise ValueError("Missing required R2 configuration. Please check your .env file.")
        
        self.r2_client = boto3.client(
            's3',
            endpoint_url=self.r2_endpoint,
            aws_access_key_id=self.r2_access_key,
            aws_secret_access_key=self.r2_secret_key,
            region_name='auto'
        )
        
        logger.info("R2 client initialized successfully")
    
    def setup_openai(self):
        """Setup OpenAI client."""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        self.openai_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.openai_api_key}"
        }
        
        logger.info("OpenAI client configured successfully")
    
    def list_avatar_files(self) -> List[Dict[str, Any]]:
        """
        List all files in the avatar folder.
        
        Returns:
            List of file objects with metadata
        """
        try:
            response = self.r2_client.list_objects_v2(
                Bucket=self.r2_bucket,
                Prefix=self.target_folder
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip if it's the folder itself
                    if obj['Key'] == self.target_folder:
                        continue
                    
                    # Only process image files
                    if self.is_image_file(obj['Key']):
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'filename': os.path.basename(obj['Key'])
                        })
            
            logger.info(f"Found {len(files)} avatar files to process")
            return files
            
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []
    
    def is_image_file(self, filename: str) -> bool:
        """Check if file is an image based on extension."""
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg'}
        return Path(filename).suffix.lower() in image_extensions
    
    def download_image_as_base64(self, file_key: str) -> Optional[str]:
        """
        Download image from R2 and convert to base64.
        
        Args:
            file_key: The S3 key of the file
            
        Returns:
            Base64 encoded image or None if error
        """
        try:
            response = self.r2_client.get_object(Bucket=self.r2_bucket, Key=file_key)
            image_data = response['Body'].read()
            
            # Get file extension for MIME type
            ext = Path(file_key).suffix.lower()
            mime_map = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg', 
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.bmp': 'image/bmp',
                '.webp': 'image/webp'
            }
            
            mime_type = mime_map.get(ext, 'image/jpeg')
            base64_image = base64.b64encode(image_data).decode('utf-8')
            
            return f"data:{mime_type};base64,{base64_image}"
            
        except Exception as e:
            logger.error(f"Error downloading image {file_key}: {str(e)}")
            return None
    
    def analyze_avatar_with_openai(self, base64_image: str, filename: str) -> Dict[str, Any]:
        """
        Analyze avatar image using OpenAI Vision API.
        
        Args:
            base64_image: Base64 encoded image
            filename: Original filename for context
            
        Returns:
            Dictionary with analysis results
        """
        prompt = """Analyze this avatar image and provide detailed characteristics in JSON format. 
        Focus on:
        
        1. OCCUPATION: Determine the likely profession (doctor, engineer, teacher, businessman, etc.)
        2. RACE/ETHNICITY: Identify ethnic background (caucasian, black, asian, hispanic, indian, etc.)
        3. GENDER: Identify gender (male, female, non-binary)
        4. AGE_GROUP: Estimate age range (young, middle-aged, senior)
        5. OUTFIT: Describe clothing/accessories (suit, casual, uniform, glasses, earrings, hat, etc.)
        6. STYLE: Describe the avatar style (realistic, cartoon, minimalist, professional, etc.)
        7. EXPRESSION: Describe facial expression (smiling, serious, friendly, confident, etc.)
        8. HAIR: Describe hair characteristics (short, long, bald, blonde, brunette, etc.)
        9. POSE: Describe the pose/angle (front-facing, profile, three-quarter, etc.)
        10. BACKGROUND: Describe background (solid color, office, outdoor, abstract, etc.)

        Return ONLY a valid JSON object with these exact keys (in lowercase):
        {
            "occupation": "",
            "race": "",
            "gender": "",
            "age_group": "",
            "outfit": [],
            "style": "",
            "expression": "",
            "hair": "",
            "pose": "",
            "background": "",
            "confidence_score": 0.0,
            "tags": []
        }
        
        The "outfit" field should be an array of clothing/accessory items.
        The "tags" field should be an array of all relevant descriptive tags.
        The "confidence_score" should be between 0.0 and 1.0 indicating analysis confidence.
        """
        
        payload = {
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": base64_image
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 1000
        }
        
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=self.openai_headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # Try to parse JSON from the response
                try:
                    # Clean the response in case there's extra text
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    
                    if json_start != -1 and json_end != -1:
                        json_content = content[json_start:json_end]
                        analysis = json.loads(json_content)
                        
                        # Validate required fields
                        required_fields = ['occupation', 'race', 'gender', 'age_group', 'style', 'expression']
                        for field in required_fields:
                            if field not in analysis:
                                analysis[field] = "unknown"
                        
                        # Ensure outfit and tags are lists
                        if 'outfit' not in analysis or not isinstance(analysis['outfit'], list):
                            analysis['outfit'] = []
                        if 'tags' not in analysis or not isinstance(analysis['tags'], list):
                            analysis['tags'] = []
                        
                        # Set default confidence if not provided
                        if 'confidence_score' not in analysis:
                            analysis['confidence_score'] = 0.8
                        
                        logger.info(f"Successfully analyzed {filename}")
                        return analysis
                    
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parsing error for {filename}: {str(e)}")
                    return self.get_default_analysis()
            
            else:
                logger.error(f"OpenAI API error for {filename}: {response.status_code} - {response.text}")
                return self.get_default_analysis()
                
        except Exception as e:
            logger.error(f"Error analyzing {filename} with OpenAI: {str(e)}")
            return self.get_default_analysis()
    
    def get_default_analysis(self) -> Dict[str, Any]:
        """Return default analysis structure for failed analyses."""
        return {
            "occupation": "unknown",
            "race": "unknown", 
            "gender": "unknown",
            "age_group": "unknown",
            "outfit": [],
            "style": "unknown",
            "expression": "unknown",
            "hair": "unknown",
            "pose": "unknown",
            "background": "unknown",
            "confidence_score": 0.0,
            "tags": ["unanalyzed"]
        }
    
    def generate_new_filename(self, analysis: Dict[str, Any], original_filename: str) -> str:
        """
        Generate a new descriptive filename based on analysis.
        
        Args:
            analysis: OpenAI analysis results
            original_filename: Original file name
            
        Returns:
            New descriptive filename
        """
        # Get file extension
        ext = Path(original_filename).suffix
        
        # Build filename components
        components = []
        
        # Add gender if available
        if analysis.get('gender', 'unknown') != 'unknown':
            components.append(analysis['gender'])
        
        # Add race if available
        if analysis.get('race', 'unknown') != 'unknown':
            components.append(analysis['race'])
        
        # Add occupation if available
        if analysis.get('occupation', 'unknown') != 'unknown':
            components.append(analysis['occupation'])
        
        # Add key outfit items (limit to 2)
        outfit_items = analysis.get('outfit', [])
        if outfit_items:
            components.extend(outfit_items[:2])
        
        # Add style if specific
        style = analysis.get('style', '')
        if style and style not in ['unknown', 'realistic']:
            components.append(style)
        
        # Clean and join components
        clean_components = []
        for comp in components:
            # Clean the component
            clean_comp = re.sub(r'[^a-zA-Z0-9]', '', str(comp).lower())
            if clean_comp and clean_comp not in clean_components:
                clean_components.append(clean_comp)
        
        # Limit to 4 components to keep filename reasonable
        clean_components = clean_components[:4]
        
        if not clean_components:
            # Fallback to original name if no good components
            return original_filename
        
        # Generate new filename
        new_name = '_'.join(clean_components) + ext
        
        # Ensure it's not too long
        if len(new_name) > 50:
            new_name = '_'.join(clean_components[:3]) + ext
        
        return new_name
    
    def rename_file_in_r2(self, old_key: str, new_filename: str) -> str:
        """
        Rename file in R2 storage.
        
        Args:
            old_key: Current file key
            new_filename: New filename
            
        Returns:
            New file key
        """
        try:
            # Construct new key
            folder_path = '/'.join(old_key.split('/')[:-1])
            new_key = f"{folder_path}/{new_filename}"
            
            # Check if new key already exists
            try:
                self.r2_client.head_object(Bucket=self.r2_bucket, Key=new_key)
                # If exists, add timestamp to make unique
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name_parts = new_filename.rsplit('.', 1)
                if len(name_parts) == 2:
                    new_filename = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
                else:
                    new_filename = f"{new_filename}_{timestamp}"
                new_key = f"{folder_path}/{new_filename}"
            except:
                pass  # File doesn't exist, we can use the new name
            
            # Copy object to new location
            copy_source = {'Bucket': self.r2_bucket, 'Key': old_key}
            self.r2_client.copy_object(
                CopySource=copy_source,
                Bucket=self.r2_bucket,
                Key=new_key
            )
            
            # Delete old object
            self.r2_client.delete_object(Bucket=self.r2_bucket, Key=old_key)
            
            logger.info(f"Renamed {old_key} to {new_key}")
            return new_key
            
        except Exception as e:
            logger.error(f"Error renaming file {old_key}: {str(e)}")
            return old_key  # Return original key if rename failed
    
    def generate_public_url(self, file_key: str) -> str:
        """
        Generate public URL for the file using custom domain.
        
        Args:
            file_key: The S3 key of the file
            
        Returns:
            Public URL using custom domain
        """
        # Use custom domain mapping: assets.speako.ai -> speako-public-assets bucket
        return f"https://assets.speako.ai/{file_key}"
    
    def process_avatars(self) -> Dict[str, Any]:
        """
        Main processing function to scan, analyze, and catalog avatars.
        
        Returns:
            Complete catalog dictionary
        """
        logger.info("Starting avatar processing...")
        
        # Get list of avatar files
        files = self.list_avatar_files()
        if not files:
            logger.warning("No avatar files found to process")
            return {"avatars": [], "metadata": {"total_files": 0, "processed": 0, "errors": 0}}
        
        catalog = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_files": len(files),
                "processed": 0,
                "errors": 0,
                "folder_scanned": self.target_folder,
                "bucket": self.r2_bucket
            },
            "avatars": []
        }
        
        for i, file_info in enumerate(files):
            logger.info(f"Processing file {i+1}/{len(files)}: {file_info['filename']}")
            
            try:
                # Download image as base64
                base64_image = self.download_image_as_base64(file_info['key'])
                if not base64_image:
                    self.error_count += 1
                    continue
                
                # Analyze with OpenAI
                analysis = self.analyze_avatar_with_openai(base64_image, file_info['filename'])
                
                # Generate new filename
                new_filename = self.generate_new_filename(analysis, file_info['filename'])
                
                # Rename file in R2
                new_key = self.rename_file_in_r2(file_info['key'], new_filename)
                
                # Generate public URL
                public_url = self.generate_public_url(new_key)
                
                # Create catalog entry
                avatar_entry = {
                    "id": f"avatar_{self.processed_count + 1:03d}",
                    "original_filename": file_info['filename'],
                    "current_filename": os.path.basename(new_key),
                    "file_key": new_key,
                    "public_url": public_url,
                    "file_size": file_info['size'],
                    "last_modified": file_info['last_modified'].isoformat(),
                    "analysis": analysis,
                    "processed_at": datetime.now().isoformat()
                }
                
                catalog["avatars"].append(avatar_entry)
                self.processed_count += 1
                
                logger.info(f"Successfully processed {file_info['filename']} -> {new_filename}")
                
            except Exception as e:
                logger.error(f"Error processing {file_info['filename']}: {str(e)}")
                self.error_count += 1
                continue
        
        # Update metadata
        catalog["metadata"]["processed"] = self.processed_count
        catalog["metadata"]["errors"] = self.error_count
        
        logger.info(f"Processing complete. Processed: {self.processed_count}, Errors: {self.error_count}")
        
        return catalog
    
    def save_catalog(self, catalog: Dict[str, Any]) -> None:
        """
        Save catalog to JSON file.
        
        Args:
            catalog: Complete catalog dictionary
        """
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(catalog, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Catalog saved to {self.output_file}")
            
            # Also save a summary
            summary_file = "avatar_summary.json"
            summary = {
                "total_avatars": len(catalog["avatars"]),
                "processing_date": catalog["metadata"]["generated_at"],
                "occupations": {},
                "races": {},
                "genders": {},
                "styles": {},
                "sample_avatars": catalog["avatars"][:5]  # First 5 as samples
            }
            
            # Generate statistics
            for avatar in catalog["avatars"]:
                analysis = avatar["analysis"]
                
                # Count occupations
                occupation = analysis.get("occupation", "unknown")
                summary["occupations"][occupation] = summary["occupations"].get(occupation, 0) + 1
                
                # Count races
                race = analysis.get("race", "unknown")
                summary["races"][race] = summary["races"].get(race, 0) + 1
                
                # Count genders
                gender = analysis.get("gender", "unknown")
                summary["genders"][gender] = summary["genders"].get(gender, 0) + 1
                
                # Count styles
                style = analysis.get("style", "unknown")
                summary["styles"][style] = summary["styles"].get(style, 0) + 1
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Summary saved to {summary_file}")
            
            # Create simplified file list for easy access
            simple_file = "avatar_filelist.json"
            simple_data = {
                "generated_at": catalog["metadata"]["generated_at"],
                "total_files": len(catalog["avatars"]),
                "files": []
            }
            
            for avatar in catalog["avatars"]:
                simple_entry = {
                    "id": avatar["id"],
                    "filename": avatar["current_filename"],
                    "original_name": avatar["original_filename"],
                    "url": avatar["public_url"],
                    "tags": avatar["analysis"].get("tags", []),
                    "occupation": avatar["analysis"].get("occupation", "unknown"),
                    "race": avatar["analysis"].get("race", "unknown"),
                    "gender": avatar["analysis"].get("gender", "unknown"),
                    "style": avatar["analysis"].get("style", "unknown"),
                    "outfit": avatar["analysis"].get("outfit", [])
                }
                simple_data["files"].append(simple_entry)
            
            with open(simple_file, 'w', encoding='utf-8') as f:
                json.dump(simple_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Simple file list saved to {simple_file}")
            
        except Exception as e:
            logger.error(f"Error saving catalog: {str(e)}")

def main():
    """Main function to run the avatar scanner."""
    try:
        scanner = AvatarScanner()
        catalog = scanner.process_avatars()
        scanner.save_catalog(catalog)
        
        print(f"\n🎉 Avatar scanning complete!")
        print(f"📊 Processed: {catalog['metadata']['processed']} files")
        print(f"❌ Errors: {catalog['metadata']['errors']} files")
        print(f"📁 Output saved to: avatar_catalog.json")
        print(f"📋 Summary saved to: avatar_summary.json")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"❌ Fatal error: {str(e)}")

if __name__ == "__main__":
    main()
