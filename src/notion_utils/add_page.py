#!/usr/bin/env python3
"""
Notion Database Page Creator with Comprehensive Search

This script allows you to:
1. Search across ALL accessible databases (private + workspace content)
2. Add a new page to the selected database with title and content
3. Background cache refresh for improved performance
4. Workspace context awareness
5. Direct database access via database ID (bypasses search)

Requirements:
- pip install notion-client fuzzywuzzy python-levenshtein

Setup:
1. Create a Notion integration at https://www.notion.so/my-integrations
2. Copy the integration token
3. Share your databases with the integration (both private and workspace)
4. Set the NOTION_TOKEN environment variable or modify the script

Usage:
- Interactive mode: python notion_page_creator.py
- Direct mode: python notion_page_creator.py --database-id YOUR_DATABASE_ID
"""

import os
import sys
import json
import time
import threading
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from .notion_page import NotionPageCreator

TODO_DATABASE_ID = '25ffd1e7c2e1800d9be9f8b38365b1c6'

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Create pages in Notion databases with fuzzy search or direct database ID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python notion_page_creator.py
    # Interactive mode with database search
    
  python notion_page_creator.py --database-id 12345678-90ab-cdef-1234-567890abcdef
    # Direct mode with database ID (bypasses search)
    
  python notion_page_creator.py -d 1234567890abcdef1234567890abcdef
    # Direct mode with database ID without dashes

Note: Database IDs can be found in the URL of your Notion database:
https://notion.so/workspace/DatabaseName-{DATABASE_ID}?v=...
        """
    )
    
    parser.add_argument(
        '-d', '--database-id',
        type=str,
        help='Database ID to create page in (bypasses search)',
        metavar='DATABASE_ID'
    )
    
    return parser.parse_args()

def get_token():

    # Get Notion token
    token = os.getenv("NOTION_TOKEN")
    
    if not token:
        print("ERROR: Notion token not found!")
        print("\nPlease set the NOTION_TOKEN environment variable:")
        print("export NOTION_TOKEN='your_notion_integration_token'")
        print("\nOr get your token from: https://www.notion.so/my-integrations")
        print("\nREMEMBER: Share both private and workspace databases with your integration!")
        
        # Allow manual token input
        token = input("\nAlternatively, enter your token here: ").strip()
        if not token:
            print("ERROR: No token provided. Exiting.")
            sys.exit(1)

    return token

def main():
    """Main function to run the script."""

    args = parse_arguments()
    token = get_token()
    creator = NotionPageCreator(token)
    creator.run(database_id=args.database_id)

def todo():
    """Main function to run the script."""

    token = get_token()
    creator = NotionPageCreator(token)
    creator.run(database_id=TODO_DATABASE_ID)


if __name__ == "__main__":
    main()


    