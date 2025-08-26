from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
import random
from typing import Dict, List, Tuple

def get_venue_tags_by_location(cur, tenant_id: int, location_id: int) -> Dict[str, int]:
    """
    Retrieve venue tags for a specific location.
    Returns a dictionary with tag names as keys and tag_ids as values.
    """
    cur.execute("""
        SELECT name, tag_id 
        FROM venue_tag 
        WHERE tenant_id = %s AND location_id = %s AND is_active = TRUE
    """, (tenant_id, location_id))
    
    tags = {}
    for name, tag_id in cur.fetchall():
        tags[name.lower()] = tag_id
    
    return tags

def get_venue_units_by_location(cur, tenant_id: int, location_id: int) -> List[Tuple[int, str]]:
    """
    Retrieve all venue units for a specific location.
    Returns a list of tuples (venue_unit_id, name).
    """
    cur.execute("""
        SELECT venue_unit_id, name 
        FROM venue_unit 
        WHERE tenant_id = %s AND location_id = %s
    """, (tenant_id, location_id))
    
    return cur.fetchall()

def update_venue_unit_zone_tags(cur, tenant_id: int, venue_unit_id: int, zone_tag_ids: List[int]):
    """
    Update the zone_tag_ids field for a specific venue unit.
    """
    # Convert list of integers to PostgreSQL array format
    zone_tag_array = '{' + ','.join(map(str, zone_tag_ids)) + '}'
    
    cur.execute("""
        UPDATE venue_unit 
        SET zone_tag_ids = %s 
        WHERE tenant_id = %s AND venue_unit_id = %s
    """, (zone_tag_array, tenant_id, venue_unit_id))

def assign_zone_tags_randomly(venue_units: List[Tuple[int, str]], inside_tag_id: int, outdoor_tag_id: int) -> Dict[int, List[int]]:
    """
    Randomly assign zone tags to venue units with approximately 70% inside and 30% outdoor.
    Returns a dictionary mapping venue_unit_id to list of zone_tag_ids.
    """
    assignments = {}
    total_units = len(venue_units)
    
    # Calculate how many units should be inside vs outdoor
    inside_count = int(total_units * 0.7)
    
    # Create a list of assignments
    assignment_list = ['inside'] * inside_count + ['outdoor'] * (total_units - inside_count)
    
    # Shuffle to randomize
    random.shuffle(assignment_list)
    
    for i, (venue_unit_id, name) in enumerate(venue_units):
        if assignment_list[i] == 'inside':
            assignments[venue_unit_id] = [inside_tag_id]
        else:
            assignments[venue_unit_id] = [outdoor_tag_id]
    
    return assignments

def manual_update_venue_tag():
    """
    Main function to update venue unit zone tags.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set in environment variables")
        return
    
    try:
        print("🔗 Connecting to PostgreSQL...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Get all locations with venue units
        cur.execute("""
            SELECT DISTINCT l.tenant_id, l.location_id, l.name as location_name
            FROM locations l
            INNER JOIN venue_unit vu ON l.tenant_id = vu.tenant_id AND l.location_id = vu.location_id
            WHERE l.location_type = 'rest'
            ORDER BY l.tenant_id, l.location_id
        """)
        
        locations = cur.fetchall()
        print(f"📍 Found {len(locations)} locations with venue units")
        
        total_updated = 0
        
        for tenant_id, location_id, location_name in locations:
            print(f"\n🏢 Processing location: {location_name} (tenant_id: {tenant_id}, location_id: {location_id})")
            
            # Get venue tags for this location
            venue_tags = get_venue_tags_by_location(cur, tenant_id, location_id)
            
            if 'inside' not in venue_tags or 'outdoor' not in venue_tags:
                print(f"⚠️  Missing required tags for location {location_name}. Available tags: {list(venue_tags.keys())}")
                continue
            
            inside_tag_id = venue_tags['inside']
            outdoor_tag_id = venue_tags['outdoor']
            print(f"   📋 Inside tag_id: {inside_tag_id}, Outdoor tag_id: {outdoor_tag_id}")
            
            # Get venue units for this location
            venue_units = get_venue_units_by_location(cur, tenant_id, location_id)
            if not venue_units:
                print(f"   ℹ️  No venue units found for location {location_name}")
                continue
            
            print(f"   🏗️  Found {len(venue_units)} venue units")
            
            # Assign zone tags randomly
            assignments = assign_zone_tags_randomly(venue_units, inside_tag_id, outdoor_tag_id)
            
            # Count assignments for reporting
            inside_count = sum(1 for tags in assignments.values() if inside_tag_id in tags)
            outdoor_count = len(assignments) - inside_count
            
            print(f"   📊 Assignment: {inside_count} inside ({inside_count/len(assignments)*100:.1f}%), {outdoor_count} outdoor ({outdoor_count/len(assignments)*100:.1f}%)")
            
            # Update venue units
            for venue_unit_id, zone_tag_ids in assignments.items():
                update_venue_unit_zone_tags(cur, tenant_id, venue_unit_id, zone_tag_ids)
                total_updated += 1
            
            print(f"   ✅ Updated {len(assignments)} venue units")
        
        # Commit the transaction
        conn.commit()
        print(f"\n🎉 Successfully updated {total_updated} venue units across {len(locations)} locations")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("🔐 Database connection closed")

if __name__ == "__main__":
    print("🚀 Starting venue unit zone tag assignment...")
    manual_update_venue_tag()
    print("✨ Done!")
