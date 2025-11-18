"""
Dispatch script for syncing ElevenLabs conversations.

This script queries all active locations with ElevenLabs agents and
dispatches Celery tasks to sync their conversations.

Usage:
    python dispatch/sync_elevenlabs_conversations_dispatch.py
"""

from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv

from tasks.sync_elevenlabs_conversations import sync_conversations_for_location

# Load environment variables
load_dotenv()


def get_db_connection():
    """Get PostgreSQL database connection."""
    try:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to database: {e}")
        raise


def fetch_locations_with_agents():
    """
    Fetch all active locations that have ElevenLabs agents configured.
    
    Returns:
        List of location dictionaries with keys:
        - tenant_id
        - location_id
        - elevenlabs_agent_id
        - timezone
        - name
    """
    locations = []
    
    try:
        conn = get_db_connection()
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    tenant_id,
                    location_id,
                    elevenlabs_agent_id,
                    timezone,
                    name
                FROM locations
                WHERE is_active = true
                  AND elevenlabs_agent_id IS NOT NULL
                  AND elevenlabs_agent_id != ''
                ORDER BY tenant_id, location_id
            """)
            
            for row in cur.fetchall():
                locations.append({
                    "tenant_id": row[0],
                    "location_id": row[1],
                    "elevenlabs_agent_id": row[2],
                    "timezone": row[3],
                    "name": row[4]
                })
        
        conn.close()
        
        return locations
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch locations from database: {e}")
        return locations


def dispatch_sync_tasks():
    """
    Main dispatch function.
    
    Queries locations and dispatches Celery tasks for each location.
    """
    print("=" * 80)
    print("[DISPATCH] ElevenLabs Conversation Sync")
    print(f"[DISPATCH] Started at: {datetime.now().isoformat()}")
    print("=" * 80)
    
    # Fetch locations
    locations = fetch_locations_with_agents()
    
    if not locations:
        print("[WARN] No active locations with ElevenLabs agents found")
        return
    
    print(f"[INFO] Found {len(locations)} locations to sync:")
    
    # Display locations
    for loc in locations:
        print(
            f"  - Tenant {loc['tenant_id']}, Location {loc['location_id']}: "
            f"{loc['name']} (Agent: {loc['elevenlabs_agent_id']}, TZ: {loc['timezone']})"
        )
    
    print()
    print("[INFO] Dispatching Celery tasks...")
    print()
    
    # Dispatch tasks
    dispatched = 0
    failed = 0
    
    for loc in locations:
        try:
            # Dispatch Celery task
            result = sync_conversations_for_location.delay(
                loc['tenant_id'],
                loc['location_id'],
                loc['elevenlabs_agent_id'],
                loc['timezone'],
                loc['name']
            )
            
            print(
                f"[DISPATCHED] Location {loc['location_id']} ({loc['name']}): "
                f"Task ID = {result.id}"
            )
            
            dispatched += 1
            
        except Exception as e:
            print(
                f"[ERROR] Failed to dispatch task for location {loc['location_id']}: {e}"
            )
            failed += 1
    
    # Summary
    print()
    print("=" * 80)
    print("[DISPATCH] Summary:")
    print(f"  Total locations: {len(locations)}")
    print(f"  Tasks dispatched: {dispatched}")
    print(f"  Failed: {failed}")
    print(f"[DISPATCH] Completed at: {datetime.now().isoformat()}")
    print("=" * 80)


if __name__ == "__main__":
    dispatch_sync_tasks()
