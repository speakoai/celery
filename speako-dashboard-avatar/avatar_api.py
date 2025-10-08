"""
Avatar API Endpoint

A Flask endpoint to serve avatar catalog data for web applications.
Provides filtering, searching, and pagination capabilities.
"""

from flask import Flask, jsonify, request
import json
import os
from typing import Dict, List, Any, Optional

class AvatarAPI:
    """API class for serving avatar catalog data."""
    
    def __init__(self, catalog_file: str = "avatar_catalog.json"):
        """Initialize with catalog file path."""
        self.catalog_file = catalog_file
        self.catalog = self.load_catalog()
    
    def load_catalog(self) -> Dict[str, Any]:
        """Load avatar catalog from JSON file."""
        try:
            if os.path.exists(self.catalog_file):
                with open(self.catalog_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {"avatars": [], "metadata": {}}
        except Exception as e:
            print(f"Error loading catalog: {str(e)}")
            return {"avatars": [], "metadata": {}}
    
    def refresh_catalog(self) -> bool:
        """Refresh catalog from file."""
        try:
            self.catalog = self.load_catalog()
            return True
        except Exception:
            return False
    
    def get_all_avatars(self) -> Dict[str, Any]:
        """Get all avatars with metadata."""
        return {
            "success": True,
            "data": self.catalog.get("avatars", []),
            "metadata": self.catalog.get("metadata", {}),
            "total": len(self.catalog.get("avatars", []))
        }
    
    def search_avatars(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search avatars based on filters.
        
        Args:
            filters: Dictionary of search criteria
            
        Returns:
            Filtered avatar results
        """
        avatars = self.catalog.get("avatars", [])
        filtered_avatars = []
        
        for avatar in avatars:
            analysis = avatar.get("analysis", {})
            
            # Apply filters
            match = True
            
            # Gender filter
            if "gender" in filters and filters["gender"]:
                if analysis.get("gender", "").lower() != filters["gender"].lower():
                    match = False
            
            # Race filter
            if "race" in filters and filters["race"]:
                if analysis.get("race", "").lower() != filters["race"].lower():
                    match = False
            
            # Occupation filter
            if "occupation" in filters and filters["occupation"]:
                if analysis.get("occupation", "").lower() != filters["occupation"].lower():
                    match = False
            
            # Style filter
            if "style" in filters and filters["style"]:
                if analysis.get("style", "").lower() != filters["style"].lower():
                    match = False
            
            # Age group filter
            if "age_group" in filters and filters["age_group"]:
                if analysis.get("age_group", "").lower() != filters["age_group"].lower():
                    match = False
            
            # Tags search (any tag matches)
            if "tags" in filters and filters["tags"]:
                search_tags = [tag.lower() for tag in filters["tags"]]
                avatar_tags = [tag.lower() for tag in analysis.get("tags", [])]
                if not any(tag in avatar_tags for tag in search_tags):
                    match = False
            
            # Outfit search
            if "outfit" in filters and filters["outfit"]:
                search_outfit = [item.lower() for item in filters["outfit"]]
                avatar_outfit = [item.lower() for item in analysis.get("outfit", [])]
                if not any(item in avatar_outfit for item in search_outfit):
                    match = False
            
            # Text search (searches in multiple fields)
            if "search" in filters and filters["search"]:
                search_term = filters["search"].lower()
                searchable_text = " ".join([
                    analysis.get("occupation", ""),
                    analysis.get("race", ""),
                    analysis.get("gender", ""),
                    analysis.get("style", ""),
                    analysis.get("expression", ""),
                    " ".join(analysis.get("tags", [])),
                    " ".join(analysis.get("outfit", []))
                ]).lower()
                
                if search_term not in searchable_text:
                    match = False
            
            if match:
                filtered_avatars.append(avatar)
        
        return {
            "success": True,
            "data": filtered_avatars,
            "total": len(filtered_avatars),
            "filters_applied": filters
        }
    
    def get_avatar_by_id(self, avatar_id: str) -> Dict[str, Any]:
        """Get specific avatar by ID."""
        avatars = self.catalog.get("avatars", [])
        
        for avatar in avatars:
            if avatar.get("id") == avatar_id:
                return {
                    "success": True,
                    "data": avatar
                }
        
        return {
            "success": False,
            "error": "Avatar not found",
            "data": None
        }
    
    def get_avatar_stats(self) -> Dict[str, Any]:
        """Get statistics about avatars."""
        avatars = self.catalog.get("avatars", [])
        
        stats = {
            "total_avatars": len(avatars),
            "occupations": {},
            "races": {},
            "genders": {},
            "styles": {},
            "age_groups": {},
            "common_tags": {},
            "common_outfits": {}
        }
        
        for avatar in avatars:
            analysis = avatar.get("analysis", {})
            
            # Count occupations
            occupation = analysis.get("occupation", "unknown")
            stats["occupations"][occupation] = stats["occupations"].get(occupation, 0) + 1
            
            # Count races
            race = analysis.get("race", "unknown")
            stats["races"][race] = stats["races"].get(race, 0) + 1
            
            # Count genders
            gender = analysis.get("gender", "unknown")
            stats["genders"][gender] = stats["genders"].get(gender, 0) + 1
            
            # Count styles
            style = analysis.get("style", "unknown")
            stats["styles"][style] = stats["styles"].get(style, 0) + 1
            
            # Count age groups
            age_group = analysis.get("age_group", "unknown")
            stats["age_groups"][age_group] = stats["age_groups"].get(age_group, 0) + 1
            
            # Count tags
            for tag in analysis.get("tags", []):
                stats["common_tags"][tag] = stats["common_tags"].get(tag, 0) + 1
            
            # Count outfit items
            for item in analysis.get("outfit", []):
                stats["common_outfits"][item] = stats["common_outfits"].get(item, 0) + 1
        
        # Sort by frequency
        for key in ["occupations", "races", "genders", "styles", "age_groups", "common_tags", "common_outfits"]:
            stats[key] = dict(sorted(stats[key].items(), key=lambda x: x[1], reverse=True))
        
        return {
            "success": True,
            "data": stats
        }

# Flask app setup
def create_avatar_api_routes(app: Flask, api_instance: AvatarAPI):
    """Add avatar API routes to Flask app."""
    
    @app.route('/api/avatars', methods=['GET'])
    def get_avatars():
        """Get all avatars or search with filters."""
        # Get query parameters
        filters = {}
        
        # Single value filters
        for param in ['gender', 'race', 'occupation', 'style', 'age_group', 'search']:
            value = request.args.get(param)
            if value:
                filters[param] = value
        
        # Multi-value filters (comma-separated)
        for param in ['tags', 'outfit']:
            values = request.args.get(param)
            if values:
                filters[param] = [v.strip() for v in values.split(',')]
        
        # Pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        per_page = min(per_page, 100)  # Limit max results
        
        # Get results
        if filters:
            result = api_instance.search_avatars(filters)
        else:
            result = api_instance.get_all_avatars()
        
        # Apply pagination
        total = result["total"]
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        result["data"] = result["data"][start_idx:end_idx]
        result["pagination"] = {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page
        }
        
        return jsonify(result)
    
    @app.route('/api/avatars/<avatar_id>', methods=['GET'])
    def get_avatar(avatar_id):
        """Get specific avatar by ID."""
        result = api_instance.get_avatar_by_id(avatar_id)
        return jsonify(result)
    
    @app.route('/api/avatars/stats', methods=['GET'])
    def get_avatar_statistics():
        """Get avatar statistics."""
        result = api_instance.get_avatar_stats()
        return jsonify(result)
    
    @app.route('/api/avatars/refresh', methods=['POST'])
    def refresh_catalog():
        """Refresh avatar catalog from file."""
        success = api_instance.refresh_catalog()
        return jsonify({
            "success": success,
            "message": "Catalog refreshed successfully" if success else "Failed to refresh catalog"
        })
    
    @app.route('/api/avatars/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        return jsonify({
            "success": True,
            "status": "healthy",
            "catalog_loaded": len(api_instance.catalog.get("avatars", [])) > 0,
            "total_avatars": len(api_instance.catalog.get("avatars", []))
        })

# Example usage in your main app.py
def add_avatar_routes_to_app(app):
    """Add avatar API routes to your existing Flask app."""
    avatar_api = AvatarAPI()
    create_avatar_api_routes(app, avatar_api)
    return avatar_api
