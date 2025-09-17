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
from notion_client import Client
from fuzzywuzzy import fuzz, process

TODO_DATABASE_ID = '25ffd1e7c2e1800d9be9f8b38365b1c6'

class NotionPageCreator:
    def __init__(self, token: str):
        """Initialize the Notion client with the provided token."""
        self.notion = Client(auth=token)
        self.databases: List[Dict[str, Any]] = []
        self.workspace_info: Dict[str, Any] = {}
        self.cache_file = Path.home() / ".notion_db_cache.json"
        self.cache_max_age = 3600  # 1 hour in seconds
        self._refresh_thread: Optional[threading.Thread] = None
        self._refresh_in_progress = False

    def get_workspace_info(self) -> Dict[str, Any]:
        """Get current workspace and user information."""
        try:
            # Get bot/integration info
            bot_info = self.notion.users.me()
            
            # Try to get workspace info from a search query
            search_response = self.notion.search(page_size=1)
            
            workspace_info = {
                'bot_name': bot_info.get('name', 'Unknown Integration'),
                'bot_id': bot_info.get('id', 'Unknown'),
                'bot_type': bot_info.get('type', 'Unknown'),
                'workspace_accessible': len(search_response.get('results', [])) > 0
            }
            
            return workspace_info
            
        except Exception as e:
            print(f"WARNING: Could not get workspace info: {e}")
            return {'error': str(e)}

    def display_workspace_context(self) -> None:
        """Display current workspace context and access information."""
        # print("Workspace Context")
        # print("-" * 30)
        
        self.workspace_info = self.get_workspace_info()
        
        # if 'error' not in self.workspace_info:
        #     print(f"Integration: {self.workspace_info.get('bot_name', 'Unknown')}")
        #     print(f"Access Type: {self.workspace_info.get('bot_type', 'Unknown')}")
        #     print(f"Workspace Access: {'Yes' if self.workspace_info.get('workspace_accessible') else 'Limited'}")
        # else:
        #     print(f"WARNING: Limited workspace info: {self.workspace_info['error']}")        
        # print()

    def validate_database_id(self, database_id: str) -> Optional[Dict[str, Any]]:
        """
        Validate a database ID and fetch the database if accessible.
        Returns the database object if valid and accessible, None otherwise.
        """
        try:
            # Clean the database ID (remove dashes if present)
            clean_id = database_id.replace('-', '')
            
            # Try to fetch the database directly
            database = self.notion.databases.retrieve(database_id=clean_id)
            
            if database and database.get('object') == 'database':
                # print(f"SUCCESS: Found database '{self.get_database_title(database)}'")
                return database
            else:
                print(f"ERROR: Invalid database response for ID: {database_id}")
                return None
                
        except Exception as e:
            print(f"ERROR: Could not access database with ID '{database_id}': {e}")
            print("HELP: Check that:")
            print("  1. The database ID is correct")
            print("  2. The database is shared with your integration")
            print("  3. You have read access to the database")
            return None

    def load_cache(self) -> bool:
        """Load database cache from file if it exists and is recent."""
        try:
            if not self.cache_file.exists():
                return False
            
            # Check if cache is recent enough
            cache_age = time.time() - self.cache_file.stat().st_mtime
            if cache_age > self.cache_max_age:
                print("Cache is outdated, will refresh...")
                return False
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            self.databases = cache_data.get('databases', [])
            cache_time = cache_data.get('timestamp', 0)
            workspace_cache = cache_data.get('workspace_info', {})
            
            if self.databases and cache_time:
                age_minutes = int(cache_age / 60)
                # print(f"Loaded {len(self.databases)} database(s) from cache ({age_minutes}m old)")
                
                if workspace_cache:
                    workspace_cache.get('bot_name', 'Unknown')
                    # print(f"Cached workspace: {workspace_cache.get('bot_name', 'Unknown')}")
                
                return True
            
            return False
            
        except (json.JSONDecodeError, KeyError, IOError) as e:
            print(f"WARNING: Cache error (will refresh): {e}")
            return False

    def save_cache(self, databases: List[Dict[str, Any]]) -> None:
        """Save database cache to file."""
        try:
            cache_data = {
                'databases': databases,
                'workspace_info': self.workspace_info,
                'timestamp': time.time()
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
            
            print(f"Cache saved with {len(databases)} database(s)")
            
        except IOError as e:
            print(f"WARNING: Could not save cache: {e}")

    def fetch_databases_comprehensive(self, silent: bool = False) -> List[Dict[str, Any]]:
        """
        Fetch ALL accessible databases from Notion workspace and private content.
        Uses multiple search strategies to ensure comprehensive coverage.
        """
        if not silent:
            print("Searching ALL accessible content (private + workspace)...")
        
        all_databases = []
        
        try:
            # Strategy 1: Direct database search (most comprehensive)
            if not silent:
                print("  Searching databases...")
            
            response = self.notion.search(
                filter={"property": "object", "value": "database"},
                page_size=100  # Maximum page size
            )
            
            all_databases.extend(response.get("results", []))
            
            # Strategy 2: Paginate through all results if there are more
            while response.get("has_more", False):
                if not silent:
                    print(f"  Loading more results... (found {len(all_databases)} so far)")
                
                response = self.notion.search(
                    filter={"property": "object", "value": "database"},
                    start_cursor=response.get("next_cursor"),
                    page_size=100
                )
                all_databases.extend(response.get("results", []))
            
            # Strategy 3: General search to catch any missed databases
            # Sometimes databases might not be caught by the filtered search
            if not silent:
                print("  Performing general search verification...")
            
            general_response = self.notion.search(page_size=100)
            
            # Filter for databases from general search
            general_databases = [
                item for item in general_response.get("results", [])
                if item.get("object") == "database"
            ]
            
            # Add any databases not already found
            existing_ids = {db.get("id") for db in all_databases}
            for db in general_databases:
                if db.get("id") not in existing_ids:
                    all_databases.append(db)
                    if not silent:
                        print(f"  Found additional database: {self.get_database_title(db)}")
            
            # Remove duplicates and sort
            unique_databases = []
            seen_ids = set()
            
            for db in all_databases:
                db_id = db.get("id")
                if db_id and db_id not in seen_ids:
                    seen_ids.add(db_id)
                    unique_databases.append(db)
            
            # Sort by title for consistent ordering
            unique_databases.sort(key=lambda x: self.get_database_title(x).lower())
            
            if not silent:
                print(f"Found {len(unique_databases)} total database(s)")
                
                # Show breakdown by access type
                private_count = 0
                shared_count = 0
                
                for db in unique_databases:
                    # Check if database appears to be private or shared
                    # (This is heuristic based on available metadata)
                    parent = db.get("parent", {})
                    if parent.get("type") == "workspace":
                        shared_count += 1
                    else:
                        private_count += 1
                
                if private_count > 0 or shared_count > 0:
                    print(f"  Breakdown: {shared_count} workspace, {private_count} private/page-based")
            
            if not unique_databases:
                if not silent:
                    print("ERROR: No databases found!")
                    print("HELP: Make sure you've shared databases with your integration:")
                    print("   1. Open each database in Notion")
                    print("   2. Click 'Share' -> 'Invite'")
                    print("   3. Add your integration")
                return []
            
            # Save to cache
            self.save_cache(unique_databases)
            
            return unique_databases
            
        except Exception as e:
            if not silent:
                print(f"ERROR: Error fetching databases: {e}")
                print("HELP: Check your integration token and database permissions")
            return []

    def refresh_cache_background(self) -> None:
        """Refresh cache in background thread."""
        if self._refresh_in_progress:
            return
        
        def refresh_worker():
            self._refresh_in_progress = True
            try:
                print("Refreshing database cache in background...")
                fresh_databases = self.fetch_databases_comprehensive(silent=True)
                if fresh_databases:
                    self.databases = fresh_databases
                    print("Background cache refresh completed")
                else:
                    print("WARNING: Background cache refresh failed")
            except Exception as e:
                print(f"WARNING: Background refresh error: {e}")
            finally:
                self._refresh_in_progress = False
        
        self._refresh_thread = threading.Thread(target=refresh_worker, daemon=True)
        self._refresh_thread.start()

    def start_background_refresh(self) -> None:
        """Start background refresh if cache is getting old."""
        if not self.cache_file.exists():
            return
        
        cache_age = time.time() - self.cache_file.stat().st_mtime
        # Start background refresh if cache is more than 30 minutes old
        if cache_age > 1800:  # 30 minutes
            self.refresh_cache_background()

    def search_databases(self, search_term: str) -> List[Tuple[Dict[str, Any], int]]:
        """
        Perform fuzzy search on database titles.
        Returns list of tuples: (database, similarity_score)
        """
        if not self.databases:
            return []
        
        # Extract database titles and create search choices
        database_choices = []
        for db in self.databases:
            title = self.get_database_title(db)
            # Also include parent context for better search
            parent_info = self.get_database_context(db)
            search_text = f"{title} {parent_info}".strip()
            database_choices.append((search_text, db))
        
        # Perform fuzzy search
        matches = process.extract(
            search_term, 
            [choice[0] for choice in database_choices], 
            scorer=fuzz.partial_ratio,
            limit=8  # Show more results for better selection
        )
        
        # Return databases with their scores
        results = []
        for match_text, score in matches:
            for search_text, db in database_choices:
                if search_text == match_text:
                    results.append((db, score))
                    break
        
        return results

    def get_database_title(self, database: Dict[str, Any]) -> str:
        """Extract the title from a database object."""
        title_property = database.get("title", [])
        if title_property and len(title_property) > 0:
            return title_property[0].get("text", {}).get("content", "Untitled Database")
        return "Untitled Database"

    def get_database_context(self, database: Dict[str, Any]) -> str:
        """Get context information about where the database is located."""
        parent = database.get("parent", {})
        parent_type = parent.get("type", "")
        
        if parent_type == "workspace":
            return "(Workspace)"
        elif parent_type == "page_id":
            return "(In Page)"
        elif parent_type == "database_id":
            return "(In Database)"
        else:
            return ""

    def display_search_results(self, results: List[Tuple[Dict[str, Any], int]]) -> Optional[Dict[str, Any]]:
        """Display search results and let user select a database."""
        if not results:
            print("ERROR: No databases found matching your search term.")
            print("HELP: Try different keywords or check if databases are shared with your integration")
            return None
        
        print(f"\nSearch Results ({len(results)} found):")
        print("-" * 60)
        
        for i, (database, score) in enumerate(results, 1):
            title = self.get_database_title(database)
            context = self.get_database_context(database)
            print(f"{i:2d}. {title} {context} (Match: {score}%)")
        
        # Let user select
        while True:
            try:
                choice = input(f"\nSelect a database (1-{len(results)}) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    return None
                
                index = int(choice) - 1
                if 0 <= index < len(results):
                    selected_db = results[index][0]
                    print(f"Selected: {self.get_database_title(selected_db)}")
                    return selected_db
                else:
                    print(f"ERROR: Please enter a number between 1 and {len(results)}")
                    
            except ValueError:
                print("ERROR: Please enter a valid number or 'q' to quit")

    def get_database_properties(self, database: Dict[str, Any]) -> Dict[str, Any]:
        """Get the properties schema of a database."""
        return database.get("properties", {})

    def create_page_properties(self, database: Dict[str, Any], title: str) -> Dict[str, Any]:
        """
        Create page properties based on database schema.
        This finds the title property and sets it.
        """
        properties = self.get_database_properties(database)
        page_properties = {}
        
        # Find the title property
        title_property_name = None
        for prop_name, prop_config in properties.items():
            if prop_config.get("type") == "title":
                title_property_name = prop_name
                break
        
        # Set the title
        if title_property_name:
            page_properties[title_property_name] = {
                "title": [
                    {
                        "text": {
                            "content": title
                        }
                    }
                ]
            }
        
        return page_properties

    def create_page_content(self, content: str) -> List[Dict[str, Any]]:
        """Convert text content to Notion blocks."""
        if not content.strip():
            return []
        
        # Split content into paragraphs
        paragraphs = content.split('\n\n')
        blocks = []
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if paragraph:
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": paragraph
                                }
                            }
                        ]
                    }
                })
        
        return blocks

    def create_page(self, database: Dict[str, Any], title: str, content: str) -> bool:
        """Create a new page in the specified database."""
        try:
            database_id = database["id"]
            
            # Prepare page properties
            properties = self.create_page_properties(database, title)
            
            # Prepare page content
            children = self.create_page_content(content)
            
            # Create the page
            page_data = {
                "parent": {"database_id": database_id},
                "properties": properties
            }
            
            if children:
                page_data["children"] = children
            
            response = self.notion.pages.create(**page_data)
            
            # Get the page URL
            page_url = response.get("url", "")
            db_title = self.get_database_title(database)
            db_context = self.get_database_context(database)
            
            # print(f"\nPage created successfully!")
            # print(f"Title: {title}")
            # print(f"Database: {db_title} {db_context}")
            print(f"New page created at: {page_url}")
            
            return True
            
        except Exception as e:
            print(f"\nERROR: Error creating page: {e}")
            print("HELP: Check if the integration has write access to this database")
            return False

    def get_user_input_with_background_refresh(self, selected_database: Dict[str, Any]) -> Tuple[str, str]:
        """Get page title and content from user while refreshing cache in background."""
        db_title = self.get_database_title(selected_database)
        db_context = self.get_database_context(selected_database)

        print(f"New page in: {db_title} {db_context}")
        
        # Start background refresh when user starts inputting
        self.start_background_refresh()
        
        title = input("Title: ").strip()
        if not title:
            print("ERROR: Title cannot be empty. Exiting.")
            return "", ""
        
        print("Multi-line page content (Ctrl+D when done):")

        # print("Enter page content (press Ctrl+D on Unix/Ctrl+Z on Windows when done):")
        # print("HELP: You can use multiple paragraphs - separate them with blank lines")
        # print("INFO: Cache will be refreshed in background while you type...")
        
        try:
            content_lines = []
            while True:
                try:
                    line = input()
                    content_lines.append(line)
                except EOFError:
                    break
            content = '\n'.join(content_lines)
            
            # Wait for background refresh to complete if it's running
            if self._refresh_thread and self._refresh_thread.is_alive():
                print("\nWaiting for cache refresh to complete...")
                self._refresh_thread.join(timeout=5)  # Wait max 5 seconds
            
            return title, content
            
        except KeyboardInterrupt:
            print("\nERROR: Operation cancelled.")
            return "", ""

    def initialize_databases(self) -> bool:
        """Initialize databases either from cache or by fetching fresh data."""
        # Try to load from cache first
        if self.load_cache():
            return True
        
        # If cache loading failed, fetch fresh data
        self.databases = self.fetch_databases_comprehensive()
        return len(self.databases) > 0

    def run(self, database_id: Optional[str] = None):
        """Main execution flow."""
        # print("Notion Database Page Creator (Private + Workspace Search)")
        # print("=" * 65)
        
        # Show workspace context
        self.display_workspace_context()
        
        selected_database = None
        
        # If database_id is provided, validate and use it directly
        if database_id:
            # print(f"Using provided database ID: {database_id}")
            selected_database = self.validate_database_id(database_id)
            
            if not selected_database:
                print("ERROR: Failed to access provided database ID. Falling back to search.")
                # Continue to search flow below
        
        # Search for database if no valid database_id was provided
        if not selected_database:
            # Initialize databases (cache or fresh fetch)
            if not self.initialize_databases():
                print("ERROR: No accessible databases found. Please check your integration setup.")
                return
            
            # Search for database
            while True:
                search_term = input("\nEnter search terms to find a database (or 'quit' to exit): ").strip()
                
                if search_term.lower() in ['quit', 'q', 'exit']:
                    print("Goodbye!")
                    return
                
                if not search_term:
                    print("ERROR: Please enter a search term.")
                    continue
                
                # Perform search
                results = self.search_databases(search_term)
                selected_database = self.display_search_results(results)
                
                if selected_database:
                    break
                
                # Ask if they want to search again
                again = input("\nWould you like to search again? (y/n): ").strip().lower()
                if again not in ['y', 'yes']:
                    print("Goodbye!")
                    return
        
        # Get page details with background refresh
        title, content = self.get_user_input_with_background_refresh(selected_database)
        
        if not title:
            return
        
        # Create the page
        # print("\nCreating page...")
        self.create_page(selected_database, title, content)
