#!/usr/bin/env python3
"""
Batch Avatar Scanner with Progress Tracking

Enhanced version for processing large volumes of avatar images with:
- Progress tracking and resume capability
- Batch processing options
- Error recovery
- Cost estimation for OpenAI API
"""

import os
import json
import time
from avatar_scanner import AvatarScanner
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_avatar_scanner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchAvatarScanner(AvatarScanner):
    """Enhanced scanner for batch processing with progress tracking."""
    
    def __init__(self, batch_size=10, resume=True):
        """
        Initialize batch scanner.
        
        Args:
            batch_size: Number of images to process in each batch
            resume: Whether to resume from previous progress
        """
        super().__init__()
        self.batch_size = batch_size
        self.resume = resume
        self.progress_file = "avatar_progress.json"
        self.processed_files = set()
        self.load_progress()
    
    def load_progress(self):
        """Load progress from previous run."""
        if self.resume and os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    self.processed_files = set(progress.get('processed_files', []))
                    logger.info(f"Resumed: {len(self.processed_files)} files already processed")
            except Exception as e:
                logger.warning(f"Could not load progress: {str(e)}")
    
    def save_progress(self):
        """Save current progress."""
        try:
            progress = {
                'processed_files': list(self.processed_files),
                'last_updated': datetime.now().isoformat(),
                'total_processed': len(self.processed_files)
            }
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {str(e)}")
    
    def estimate_cost_and_time(self, file_count):
        """Estimate OpenAI API cost and processing time."""
        # GPT-4 Vision pricing (approximate)
        cost_per_image = 0.01  # $0.01 per image (rough estimate)
        time_per_image = 8  # 8 seconds per image (including API call)
        
        total_cost = file_count * cost_per_image
        total_time_minutes = (file_count * time_per_image) / 60
        
        print(f"\n💰 Cost Estimation:")
        print(f"   Files to process: {file_count}")
        print(f"   Estimated cost: ${total_cost:.2f}")
        print(f"   Estimated time: {total_time_minutes:.1f} minutes")
        print(f"   Batch size: {self.batch_size} images per batch")
        print(f"   Number of batches: {(file_count + self.batch_size - 1) // self.batch_size}")
        
        return total_cost, total_time_minutes
    
    def process_avatars_batch(self, max_files=None, dry_run=False):
        """
        Process avatars in batches with progress tracking.
        
        Args:
            max_files: Maximum number of files to process (None for all)
            dry_run: If True, only show what would be processed
        """
        logger.info("Starting batch avatar processing...")
        
        # Get list of avatar files
        all_files = self.list_avatar_files()
        if not all_files:
            logger.warning("No avatar files found to process")
            return {"avatars": [], "metadata": {"total_files": 0, "processed": 0, "errors": 0}}
        
        # Filter out already processed files
        remaining_files = [f for f in all_files if f['key'] not in self.processed_files]
        
        if max_files:
            remaining_files = remaining_files[:max_files]
        
        print(f"\n📊 Processing Summary:")
        print(f"   Total files in bucket: {len(all_files)}")
        print(f"   Already processed: {len(self.processed_files)}")
        print(f"   Remaining to process: {len(remaining_files)}")
        
        if not remaining_files:
            print("✅ All files have been processed!")
            return self.load_existing_catalog()
        
        # Cost and time estimation
        cost, time_minutes = self.estimate_cost_and_time(len(remaining_files))
        
        if dry_run:
            print(f"\n🔍 DRY RUN MODE - No files will be processed")
            print(f"Files that would be processed:")
            for i, file_info in enumerate(remaining_files[:10], 1):
                print(f"  {i}. {file_info['filename']}")
            if len(remaining_files) > 10:
                print(f"  ... and {len(remaining_files) - 10} more files")
            return
        
        # Confirm processing
        if len(remaining_files) > 5:
            response = input(f"\nProceed with processing {len(remaining_files)} files? (y/N): ")
            if response.lower() != 'y':
                print("Processing cancelled.")
                return
        
        # Load existing catalog or create new one
        catalog = self.load_existing_catalog()
        if not catalog.get("avatars"):
            catalog = {
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "total_files": len(all_files),
                    "processed": 0,
                    "errors": 0,
                    "folder_scanned": self.target_folder,
                    "bucket": self.r2_bucket
                },
                "avatars": []
            }
        
        # Process in batches
        start_time = time.time()
        
        for batch_num, i in enumerate(range(0, len(remaining_files), self.batch_size), 1):
            batch_files = remaining_files[i:i + self.batch_size]
            
            print(f"\n🔄 Processing Batch {batch_num}/{(len(remaining_files) + self.batch_size - 1) // self.batch_size}")
            print(f"   Files {i+1}-{min(i + self.batch_size, len(remaining_files))} of {len(remaining_files)}")
            
            batch_start = time.time()
            
            for j, file_info in enumerate(batch_files, 1):
                file_progress = i + j
                print(f"\n   📷 Processing {file_progress}/{len(remaining_files)}: {file_info['filename']}")
                
                try:
                    # Download image as base64
                    base64_image = self.download_image_as_base64(file_info['key'])
                    if not base64_image:
                        self.error_count += 1
                        continue
                    
                    # Analyze with OpenAI
                    print(f"      🤖 Analyzing with OpenAI...")
                    analysis = self.analyze_avatar_with_openai(base64_image, file_info['filename'])
                    
                    # Generate new filename
                    new_filename = self.generate_new_filename(analysis, file_info['filename'])
                    print(f"      📝 New filename: {new_filename}")
                    
                    # Rename file in R2
                    print(f"      ☁️  Renaming in R2...")
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
                    self.processed_files.add(file_info['key'])
                    
                    print(f"      ✅ Success! Confidence: {analysis.get('confidence_score', 0):.2f}")
                    
                    # Small delay to be nice to APIs
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing {file_info['filename']}: {str(e)}")
                    self.error_count += 1
                    continue
            
            # Save progress after each batch
            self.save_progress()
            self.save_partial_catalog(catalog)
            
            batch_time = time.time() - batch_start
            elapsed_time = time.time() - start_time
            avg_time_per_file = elapsed_time / (file_progress) if file_progress > 0 else 0
            remaining_files_count = len(remaining_files) - file_progress
            eta_minutes = (remaining_files_count * avg_time_per_file) / 60
            
            print(f"\n   📊 Batch {batch_num} Complete:")
            print(f"      Batch time: {batch_time:.1f}s")
            print(f"      Average per file: {avg_time_per_file:.1f}s")
            print(f"      Files remaining: {remaining_files_count}")
            print(f"      ETA: {eta_minutes:.1f} minutes")
            
            # Optional pause between batches
            if batch_num < (len(remaining_files) + self.batch_size - 1) // self.batch_size:
                print(f"   ⏸️  Pausing 5 seconds between batches...")
                time.sleep(5)
        
        # Update final metadata
        catalog["metadata"]["processed"] = len(catalog["avatars"])
        catalog["metadata"]["errors"] = self.error_count
        catalog["metadata"]["last_updated"] = datetime.now().isoformat()
        
        total_time = time.time() - start_time
        print(f"\n🎉 Batch processing complete!")
        print(f"   Total time: {total_time/60:.1f} minutes")
        print(f"   Files processed: {len(remaining_files)}")
        print(f"   Success rate: {((len(remaining_files) - self.error_count) / len(remaining_files) * 100):.1f}%")
        
        return catalog
    
    def load_existing_catalog(self):
        """Load existing catalog if it exists."""
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load existing catalog: {str(e)}")
        return {}
    
    def save_partial_catalog(self, catalog):
        """Save catalog after each batch for recovery purposes."""
        backup_file = f"avatar_catalog_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(catalog, f, indent=2, ensure_ascii=False, default=str)
            
            # Also update main catalog
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(catalog, f, indent=2, ensure_ascii=False, default=str)
                
        except Exception as e:
            logger.error(f"Error saving partial catalog: {str(e)}")

def main():
    """Main function with interactive options."""
    print("🚀 Batch Avatar Scanner")
    print("=" * 50)
    
    # Configuration options
    print("\nConfiguration Options:")
    print("1. Full processing (all files)")
    print("2. Test run (5 files only)")
    print("3. Dry run (show what would be processed)")
    print("4. Resume previous session")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    batch_size = 5  # Small batches to be safe
    max_files = None
    dry_run = False
    resume = True
    
    if choice == "1":
        max_files = None
        print("Selected: Full processing of all files")
    elif choice == "2":
        max_files = 5
        print("Selected: Test run with 5 files")
    elif choice == "3":
        dry_run = True
        print("Selected: Dry run mode")
    elif choice == "4":
        resume = True
        print("Selected: Resume previous session")
    else:
        print("Invalid choice, defaulting to test run")
        max_files = 5
    
    try:
        scanner = BatchAvatarScanner(batch_size=batch_size, resume=resume)
        
        if dry_run:
            scanner.process_avatars_batch(max_files=max_files, dry_run=True)
        else:
            catalog = scanner.process_avatars_batch(max_files=max_files)
            if catalog:
                scanner.save_catalog(catalog)
        
    except KeyboardInterrupt:
        print("\n⏹️  Processing interrupted by user")
        print("   Progress has been saved. You can resume later.")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"❌ Fatal error: {str(e)}")

if __name__ == "__main__":
    main()
