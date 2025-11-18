"""
Dispatch script for generating dashboard metrics for all tenants.

This script queries all active tenants and dispatches Celery tasks to generate
their dashboard metrics (summary cards and booking trends).

Usage:
    python dispatch/generate_dashboard_metrics_dispatch.py
"""

from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from tasks.generate_dashboard_metrics import generate_dashboard_metrics_for_tenant


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


def fetch_active_tenants():
    """
    Fetch all active tenants from the database.
    
    Returns:
        List of tenant dictionaries with keys:
        - tenant_id
        - company_name
        - created_at
    """
    tenants = []
    
    try:
        conn = get_db_connection()
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    tenant_id,
                    company_name,
                    created_at
                FROM tenants
                WHERE is_active = true
                ORDER BY tenant_id
            """)
            
            for row in cur.fetchall():
                tenants.append({
                    "tenant_id": row[0],
                    "company_name": row[1],
                    "created_at": row[2]
                })
        
        conn.close()
        
        return tenants
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch tenants from database: {e}")
        return tenants


def dispatch_metrics_generation():
    """
    Main dispatch function.
    
    Queries all active tenants and dispatches Celery tasks for each tenant.
    """
    print("=" * 80)
    print("[DISPATCH] Dashboard Metrics Generation")
    print(f"[DISPATCH] Started at: {datetime.now().isoformat()}")
    print("=" * 80)
    
    # Fetch tenants
    tenants = fetch_active_tenants()
    
    if not tenants:
        print("[WARN] No active tenants found")
        return
    
    print(f"[INFO] Found {len(tenants)} active tenant(s):")
    
    # Display tenants
    for tenant in tenants:
        print(
            f"  - Tenant {tenant['tenant_id']}: "
            f"{tenant['company_name']} (Created: {tenant['created_at']})"
        )
    
    print()
    print("[INFO] Dispatching Celery tasks...")
    print()
    
    # Dispatch tasks
    dispatched = 0
    failed = 0
    
    for tenant in tenants:
        try:
            # Dispatch Celery task
            result = generate_dashboard_metrics_for_tenant.delay(
                tenant['tenant_id']
            )
            
            print(
                f"[DISPATCHED] Tenant {tenant['tenant_id']} ({tenant['company_name']}): "
                f"Task ID = {result.id}"
            )
            
            dispatched += 1
            
        except Exception as e:
            print(
                f"[ERROR] Failed to dispatch task for tenant {tenant['tenant_id']}: {e}"
            )
            failed += 1
    
    # Summary
    print()
    print("=" * 80)
    print("[DISPATCH] Summary:")
    print(f"  Total tenants: {len(tenants)}")
    print(f"  Tasks dispatched: {dispatched}")
    print(f"  Failed: {failed}")
    print(f"[DISPATCH] Completed at: {datetime.now().isoformat()}")
    print("=" * 80)


if __name__ == "__main__":
    dispatch_metrics_generation()
