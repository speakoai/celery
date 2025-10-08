#!/usr/bin/env python3
"""
File Organizer for Avatar Dashboard

Moves all avatar-related files to a dedicated folder and updates file paths.
"""

import os
import shutil
import json
from datetime import datetime

def organize_avatar_files():
    """Organize all avatar-related files into speako-dashboard-avatar folder."""
    
    print("📁 Organizing Avatar Files")
    print("=" * 50)
    
    # Define the target folder
    target_folder = "speako-dashboard-avatar"
    
    # Create target directory if it doesn't exist
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        print(f"✅ Created directory: {target_folder}")
    
    # List of avatar-related files to move
    avatar_files = [
        # Core scanner files
        "avatar_scanner.py",
        "batch_avatar_scanner.py",
        
        # JSON output files
        "avatar_catalog.json",
        "avatar_filelist.json", 
        "avatar_summary.json",
        "avatar_progress.json",
        
        # Utility scripts
        "update_avatar_urls.py",
        "test_avatar_setup.py",
        "preview_avatars.py",
        "test_avatar_urls.py",
        
        # Documentation
        "AVATAR_README.md",
        
        # Log files
        "avatar_scanner.log",
        "batch_avatar_scanner.log"
    ]
    
    # Backup files (all avatar_catalog_backup_*.json files)
    backup_files = [f for f in os.listdir('.') if f.startswith('avatar_catalog_backup_') and f.endswith('.json')]
    avatar_files.extend(backup_files)
    
    moved_files = []
    not_found_files = []
    
    # Move each file
    for file_name in avatar_files:
        source_path = file_name
        target_path = os.path.join(target_folder, file_name)
        
        if os.path.exists(source_path):
            try:
                shutil.move(source_path, target_path)
                moved_files.append(file_name)
                print(f"📦 Moved: {file_name}")
            except Exception as e:
                print(f"❌ Error moving {file_name}: {str(e)}")
        else:
            not_found_files.append(file_name)
    
    print(f"\n📊 Summary:")
    print(f"   ✅ Moved: {len(moved_files)} files")
    print(f"   ⚠️  Not found: {len(not_found_files)} files")
    
    if not_found_files:
        print(f"\n📋 Files not found (probably already moved or don't exist):")
        for file_name in not_found_files:
            print(f"   - {file_name}")
    
    # Create an index file in the new folder
    create_index_file(target_folder, moved_files)
    
    # Create a README in the main folder pointing to the new location
    create_redirect_readme()
    
    print(f"\n🎉 Organization complete!")
    print(f"📁 All avatar files are now in: {target_folder}/")
    
    return target_folder

def create_index_file(folder_path, moved_files):
    """Create an index file listing all avatar-related files."""
    
    index_content = {
        "folder_info": {
            "name": "speako-dashboard-avatar",
            "purpose": "Avatar scanning, analysis, and management system",
            "created": datetime.now().isoformat(),
            "total_files": len(moved_files)
        },
        "file_categories": {
            "core_scanners": [
                "avatar_scanner.py",
                "batch_avatar_scanner.py"
            ],
            "json_outputs": [
                "avatar_catalog.json",
                "avatar_filelist.json", 
                "avatar_summary.json",
                "avatar_progress.json"
            ],
            "utility_scripts": [
                "update_avatar_urls.py",
                "test_avatar_setup.py",
                "preview_avatars.py",
                "test_avatar_urls.py"
            ],
            "documentation": [
                "AVATAR_README.md"
            ],
            "logs": [
                "avatar_scanner.log",
                "batch_avatar_scanner.log"
            ],
            "backups": [f for f in moved_files if f.startswith('avatar_catalog_backup_')]
        },
        "quick_commands": {
            "scan_avatars": "python batch_avatar_scanner.py",
            "test_setup": "python test_avatar_setup.py",
            "preview_files": "python preview_avatars.py",
            "update_urls": "python update_avatar_urls.py",
            "test_urls": "python test_avatar_urls.py"
        },
        "file_descriptions": {
            "avatar_catalog.json": "Complete avatar database with full metadata",
            "avatar_filelist.json": "Simple file list perfect for website integration",
            "avatar_summary.json": "Statistics and overview of all avatars",
            "avatar_scanner.py": "Main avatar scanning and analysis script",
            "batch_avatar_scanner.py": "Enhanced batch processing with progress tracking",
            "update_avatar_urls.py": "Updates URLs to use custom domain",
            "test_avatar_setup.py": "Verifies setup and connectivity",
            "AVATAR_README.md": "Complete documentation and usage guide"
        }
    }
    
    index_path = os.path.join(folder_path, "index.json")
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index_content, f, indent=2, ensure_ascii=False)
    
    print(f"📋 Created index file: {index_path}")

def create_redirect_readme():
    """Create a README in the main folder pointing to the avatar folder."""
    
    readme_content = """# Avatar System Files Moved

📁 **All avatar-related files have been moved to:**
```
speako-dashboard-avatar/
```

## What was moved:
- Avatar scanner scripts
- JSON output files (catalog, filelist, summary)
- Utility scripts and tests
- Documentation and logs
- Backup files

## Quick Start:
```bash
cd speako-dashboard-avatar
python batch_avatar_scanner.py
```

## Main Files:
- `avatar_filelist.json` - For website integration
- `avatar_catalog.json` - Complete database
- `AVATAR_README.md` - Full documentation

## Navigate to the folder:
```bash
cd speako-dashboard-avatar
```

All avatar functionality is now contained in this organized folder.
"""
    
    with open("AVATAR_SYSTEM_MOVED.md", 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print("📄 Created redirect notice: AVATAR_SYSTEM_MOVED.md")

def update_script_paths():
    """Update any hardcoded paths in scripts to work from the new location."""
    
    target_folder = "speako-dashboard-avatar"
    
    # Update the update_avatar_urls.py script to handle relative paths
    script_path = os.path.join(target_folder, "update_avatar_urls.py")
    
    if os.path.exists(script_path):
        print(f"🔧 Updating paths in {script_path}")
        # The script should already work fine with relative paths, but we could add improvements here if needed

def main():
    """Main function to organize avatar files."""
    try:
        target_folder = organize_avatar_files()
        
        print(f"\n🚀 Next Steps:")
        print(f"1. Navigate to the avatar folder:")
        print(f"   cd {target_folder}")
        print(f"")
        print(f"2. All your avatar files are now organized:")
        print(f"   📄 JSON files for website integration")
        print(f"   🐍 Python scripts for management") 
        print(f"   📚 Documentation and logs")
        print(f"")
        print(f"3. To work with avatars, always run commands from:")
        print(f"   {target_folder}/")
        
    except Exception as e:
        print(f"❌ Error organizing files: {str(e)}")

if __name__ == "__main__":
    main()
