# import requests
# import os
# import time
# from tqdm import tqdm

# def add_category_tags(database_id, token, gene_categories, 
#                       multiselect_column="gene_sets"):
#     """
#     Add different tags based on gene categories
    
#     gene_categories: dict of category_name -> list of genes
#     Example: {
#         "Oncogene": ["KRAS", "MYC", "EGFR"],
#         "Tumor Suppressor": ["TP53", "PTEN", "RB1"],
#         "DNA Repair": ["BRCA1", "BRCA2", "MLH1"]
#     }
#     """
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Content-Type": "application/json",
#         "Notion-Version": "2022-06-28"
#     }
    
#     # Create reverse mapping: gene -> categories
#     gene_to_categories = {}
#     for category, genes in gene_categories.items():
#         for gene in genes:
#             if gene not in gene_to_categories:
#                 gene_to_categories[gene] = []
#             gene_to_categories[gene].append(category)

#     # Ensure all category tags exist in database
#     print("Updating database schema with category tags...")
#     all_tags = list(gene_categories.keys())
    
#     # Get current database schema
#     db_response = requests.get(
#         f"https://api.notion.com/v1/databases/{database_id}",
#         headers=headers
#     )
#     database = db_response.json()
    
#     existing_options = []
#     if multiselect_column in database["properties"]:
#         existing_options = database["properties"][multiselect_column].get("multi_select", {}).get("options", [])
    
#     existing_names = {opt["name"] for opt in existing_options}
    
#     # Add new tags with colors
#     colors = ["red", "blue", "green", "yellow", "purple", "pink", "orange", "brown"]
#     new_options = existing_options.copy()
    
#     for i, tag in enumerate(all_tags):
#         if tag not in existing_names:
#             new_options.append({
#                 "name": tag,
#                 "color": colors[i % len(colors)]
#             })
    
#     if len(new_options) > len(existing_options):
#         update_data = {
#             "properties": {
#                 multiselect_column: {
#                     "multi_select": {
#                         "options": new_options
#                     }
#                 }
#             }
#         }
#         requests.patch(
#             f"https://api.notion.com/v1/databases/{database_id}",
#             headers=headers,
#             json=update_data
#         )
    
#     # Query and update all pages
#     print("Downloading pages...")
#     query_url = f"https://api.notion.com/v1/databases/{database_id}/query"
#     response = requests.post(query_url, headers=headers)
#     pages = response.json().get("results", [])
    
#     print("Processing pages...")
#     for page in tqdm(pages):
#         page_id = page["id"]
        
#         # # Get gene name
#         # gene_name = None
#         # if "gene_name" in page["properties"]:
#         #     prop = page["properties"]["gene_name"]
#         #     if prop["type"] == "title" and prop["title"]:
#         #         gene_name = prop["title"][0]["plain_text"]

#         # Get gene name
#         gene_name = None
#         if "gene_name" in page["properties"]:
#             prop = page["properties"]["gene_name"]
#             if prop["type"] == "rich_text" and prop["rich_text"]:
#                 gene_name = prop["rich_text"][0]["plain_text"]

#         if gene_name is None:
#             print('skipping:', prop)
#             continue

#         # Get existing tags (preserve non-category tags)
#         existing_tags = []
#         if multiselect_column in page["properties"]:
#             multi_prop = page["properties"][multiselect_column]
#             if multi_prop["type"] == "multi_select":
#                 existing_tags = [tag["name"] for tag in multi_prop["multi_select"]]
        
#         # Remove old category tags and add new ones
#         preserved_tags = [tag for tag in existing_tags if tag not in all_tags]
        
#         # Add appropriate category tags
#         if gene_name in gene_to_categories:
#             new_tags = preserved_tags + gene_to_categories[gene_name]
#         else:
#             new_tags = preserved_tags

#         # Only update if tags changed
#         if set(new_tags) != set(existing_tags):
#             print(f"Updating {gene_name}: {', '.join(new_tags)}")
#             update_page_data = {
#                 "properties": {
#                     multiselect_column: {
#                         "multi_select": [{"name": tag} for tag in new_tags]
#                     }
#                 }
#             }
            
#             response = requests.patch(
#                 f"https://api.notion.com/v1/pages/{page_id}",
#                 headers=headers,
#                 json=update_page_data
#             )
            
#             if response.status_code == 200:
#                 print(f"Updated {gene_name}: {', '.join(new_tags)}")
        
#         time.sleep(0.35)

# # Example usage with categories
# gene_categories = {
#     "Helloooo": ["ABCB7"],
#     # "Oncogene": ["KRAS", "MYC", "EGFR", "BRAF", "ALK"],
#     # "Tumor Suppressor": ["TP53", "PTEN", "RB1", "BRCA1", "BRCA2"],
#     # "DNA Repair": ["BRCA1", "BRCA2", "MLH1", "MSH2"],
#     # "Cell Cycle": ["CDK4", "CCND1", "RB1"]
# }


# add_category_tags('258fd1e7c2e180ba8ea0d909f6b739b3', os.getenv('NOTION_API_KEY'), gene_categories)




import requests
import os
import time
from typing import Dict, List, Set
from datetime import datetime

class RateLimiter:
    """Simple rate limiter to ensure max 3 requests per second"""
    def __init__(self, max_per_second=3):
        self.max_per_second = max_per_second
        self.min_interval = 1.0 / max_per_second  # 0.333... seconds
        self.last_request_time = 0
    
    def wait_if_needed(self):
        """Wait if necessary to maintain rate limit"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

def add_category_tags(database_id: str, 
                      token: str, 
                      gene_categories: Dict[str, List[str]], 
                      multiselect_column: str,
                      batch_size: int = 100,
                      dry_run: bool = False) -> Dict:
    """
    Add category tags to genes in a Notion database with full pagination support.
    
    Args:
        database_id: Notion database ID
        token: Notion integration token
        gene_categories: Dict mapping category names to lists of genes
        multiselect_column: Name of the multi-select column to update
        batch_size: Number of pages to fetch per query (max 100)
        dry_run: If True, only simulate changes without updating
    
    Returns:
        Dict with statistics about the operation
    """
    
    # Initialize rate limiter
    rate_limiter = RateLimiter(max_per_second=3)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Create reverse mapping: gene -> categories
    gene_to_categories = {}
    for category, genes in gene_categories.items():
        for gene in genes:
            if gene not in gene_to_categories:
                gene_to_categories[gene] = []
            gene_to_categories[gene].append(category)
    
    print(f"Prepared {len(gene_to_categories)} genes with categories")
    
    # Step 1: Ensure all category tags exist in database schema
    print("\n1. Updating database schema with category tags...")
    rate_limiter.wait_if_needed()
    
    all_tags = list(gene_categories.keys())
    
    # Get current database schema
    db_response = requests.get(
        f"https://api.notion.com/v1/databases/{database_id}",
        headers=headers
    )
    
    if db_response.status_code != 200:
        raise Exception(f"Failed to fetch database: {db_response.text}")
    
    database = db_response.json()
    
    # Check existing multi-select options
    existing_options = []
    if multiselect_column in database["properties"]:
        prop_config = database["properties"][multiselect_column]
        if prop_config["type"] == "multi_select":
            existing_options = prop_config.get("multi_select", {}).get("options", [])
        else:
            raise ValueError(f"Column '{multiselect_column}' is not a multi-select type")
    else:
        print(f"Creating new multi-select column: {multiselect_column}")
    
    existing_names = {opt["name"] for opt in existing_options}
    
    # Add new tags with colors
    colors = ["red", "blue", "green", "yellow", "purple", "pink", "orange", "brown", "gray"]
    new_options = existing_options.copy()
    
    tags_to_add = []
    for i, tag in enumerate(all_tags):
        if tag not in existing_names:
            new_options.append({
                "name": tag,
                "color": colors[i % len(colors)]
            })
            tags_to_add.append(tag)
    
    if tags_to_add and not dry_run:
        print(f"Adding {len(tags_to_add)} new tags: {', '.join(tags_to_add)}")
        rate_limiter.wait_if_needed()
        
        update_data = {
            "properties": {
                multiselect_column: {
                    "multi_select": {
                        "options": new_options
                    }
                }
            }
        }
        
        update_response = requests.patch(
            f"https://api.notion.com/v1/databases/{database_id}",
            headers=headers,
            json=update_data
        )
        
        if update_response.status_code != 200:
            raise Exception(f"Failed to update database schema: {update_response.text}")
    else:
        print(f"All tags already exist in schema (or dry run mode)")
    
    # Step 2: Fetch ALL pages with pagination
    print(f"\n2. Fetching all pages from database (batch size: {batch_size})...")
    
    query_url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_pages = []
    has_more = True
    start_cursor = None
    batch_num = 0
    
    while has_more:
        batch_num += 1
        rate_limiter.wait_if_needed()
        
        # Prepare query data
        query_data = {
            "page_size": min(batch_size, 100)  # Notion's max is 100
        }
        if start_cursor:
            query_data["start_cursor"] = start_cursor
        
        # Fetch batch
        print(f"  Fetching batch {batch_num}...", end="")
        response = requests.post(query_url, headers=headers, json=query_data)
        
        if response.status_code != 200:
            raise Exception(f"Failed to query database: {response.text}")
        
        data = response.json()
        batch_pages = data.get("results", [])
        all_pages.extend(batch_pages)
        
        print(f" got {len(batch_pages)} pages (total: {len(all_pages)})")
        
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")


        break
    
    print(f"\nTotal pages fetched: {len(all_pages)}")
    
    # Step 3: Process and update each page
    print(f"\n3. Processing {len(all_pages)} pages...")
    
    # Statistics
    stats = {
        "total_pages": len(all_pages),
        "pages_updated": 0,
        "pages_skipped": 0,
        "genes_found": 0,
        "genes_with_categories": 0,
        "tags_added": 0,
        "tags_removed": 0,
        "errors": []
    }
    
    # Process each page
    for i, page in enumerate(all_pages, 1):
        page_id = page["id"]
        properties = page.get("properties", {})
        
        # Get gene name from various possible property types
        gene_name = None
        gene_property_name = None
        
        # Try common property names for gene
        for prop_name in ["gene_name", "Gene Name", "Gene", "Name", "Title"]:
            if prop_name in properties:
                prop = properties[prop_name]

        # # Get gene name
        # gene_name = None
        # if "gene_name" in page["properties"]:
        #     prop = page["properties"]["gene_name"]
        #     if prop["type"] == "rich_text" and prop["rich_text"]:
        #         gene_name = prop["rich_text"][0]["plain_text"]

                if prop["type"] == "title" and prop.get("title"):
                    gene_name = prop["title"][0]["plain_text"]
                    gene_property_name = prop_name
                    break
                elif prop["type"] == "rich_text" and prop.get("rich_text"):
                    gene_name = prop["rich_text"][0]["plain_text"]
                    gene_property_name = prop_name
                    break
                elif prop["type"] == "select" and prop.get("select"):
                    gene_name = prop["select"]["name"]
                    gene_property_name = prop_name
                    break
        
        if gene_name:
            stats["genes_found"] += 1
        print(gene_name)
        # Get existing tags from the multi-select column
        existing_tags = []
        if multiselect_column in properties:
            multi_prop = properties[multiselect_column]
            if multi_prop["type"] == "multi_select":
                existing_tags = [tag["name"] for tag in multi_prop.get("multi_select", [])]
        
        # Calculate new tags
        preserved_tags = [tag for tag in existing_tags if tag not in all_tags]
        category_tags = []
        
        if gene_name and gene_name in gene_to_categories:
            category_tags = gene_to_categories[gene_name]
            stats["genes_with_categories"] += 1
        
        new_tags = preserved_tags + category_tags
        
        # Check if update is needed
        existing_set = set(existing_tags)
        new_set = set(new_tags)
        
        if existing_set != new_set:
            # Calculate what's being added/removed
            added = new_set - existing_set
            removed = existing_set - new_set
            stats["tags_added"] += len(added)
            stats["tags_removed"] += len(removed)
            
            if not dry_run:
                # Update the page
                rate_limiter.wait_if_needed()
                
                update_page_data = {
                    "properties": {
                        multiselect_column: {
                            "multi_select": [{"name": tag} for tag in new_tags]
                        }
                    }
                }
                
                update_response = requests.patch(
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=headers,
                    json=update_page_data
                )
                
                if update_response.status_code == 200:
                    stats["pages_updated"] += 1
                    action_desc = []
                    if added:
                        action_desc.append(f"+{list(added)}")
                    if removed:
                        action_desc.append(f"-{list(removed)}")
                    
                    print(f"  [{i}/{len(all_pages)}] {gene_name or 'Unknown'}: {' '.join(action_desc)}")
                else:
                    error_msg = f"Failed to update page {page_id}: {update_response.text}"
                    stats["errors"].append(error_msg)
                    print(f"  [{i}/{len(all_pages)}] ERROR: {error_msg}")
            else:
                stats["pages_updated"] += 1
                print(f"  [{i}/{len(all_pages)}] {gene_name or 'Unknown'}: "
                      f"Would update tags (dry run)")
        else:
            stats["pages_skipped"] += 1
            if (i % 100) == 0:  # Progress indicator every 100 pages
                print(f"  [{i}/{len(all_pages)}] Processed...")
    
    # Print summary
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Total pages processed: {stats['total_pages']}")
    print(f"Pages updated: {stats['pages_updated']}")
    print(f"Pages skipped (no change): {stats['pages_skipped']}")
    print(f"Genes found: {stats['genes_found']}")
    print(f"Genes with categories: {stats['genes_with_categories']}")
    print(f"Total tags added: {stats['tags_added']}")
    print(f"Total tags removed: {stats['tags_removed']}")
    
    if stats['errors']:
        print(f"\nErrors encountered: {len(stats['errors'])}")
        for error in stats['errors'][:5]:  # Show first 5 errors
            print(f"  - {error}")
        if len(stats['errors']) > 5:
            print(f"  ... and {len(stats['errors']) - 5} more")
    
    if dry_run:
        print("\n** DRY RUN MODE - No actual changes were made **")
    
    return stats


def add_tags_to_gene_list():
    
    # gene_tags = {
    #     "Tester": ["ABCB7"],
    #     # "Oncogene": ["KRAS", "MYC", "EGFR", "BRAF", "ALK", "HER2", "MET"],
    #     # "Tumor Suppressor": ["TP53", "PTEN", "RB1", "BRCA1", "BRCA2", "APC", "VHL"],
    #     # "DNA Repair": ["BRCA1", "BRCA2", "MLH1", "MSH2", "ATM", "CHEK2"],
    #     # "Cell Cycle": ["CDK4", "CCND1", "RB1", "CDKN2A", "CCNE1"],
    #     # "Apoptosis": ["BCL2", "BAX", "CASP3", "CASP9", "FAS"],
    # }
    # with open('data.yml', 'w') as outfile:
    #     yaml.dump(gene_tags, outfile, default_flow_style=False)

    from pathlib import Path
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_id', type=str, required=False, default="258fd1e7c2e180ba8ea0d909f6b739b3", help="Notion database ID")
    parser.add_argument('--token', type=str, required=False, help="Notion integration token")
    parser.add_argument('--column', type=str, required=False, help="Multiselect column in Notion database to add tags to")
    parser.add_argument('input_file_path', type=Path, help="Yaml file with gene categories")
    args = parser.parse_args()

    with open(args.input_file_path, 'r') as f:
       gene_tags = yaml.safe_load(f)

    add_category_tags(
        database_id=args.database_id,
        token=args.token,
        gene_categories=gene_tags,
        multiselect_column="protein_gene_sets",
        batch_size=10,
        dry_run=False
    )

if __name__ == "__main__":
    add_tags_to_gene_list()



    # DATABASE_ID = "your_database_id_here"
    # NOTION_TOKEN = "your_integration_token_here"
    
    # # Define gene categories
    # gene_categories = {
    #     "Tester": ["ABCB7"],
    #     # "Oncogene": ["KRAS", "MYC", "EGFR", "BRAF", "ALK", "HER2", "MET"],
    #     # "Tumor Suppressor": ["TP53", "PTEN", "RB1", "BRCA1", "BRCA2", "APC", "VHL"],
    #     # "DNA Repair": ["BRCA1", "BRCA2", "MLH1", "MSH2", "ATM", "CHEK2"],
    #     # "Cell Cycle": ["CDK4", "CCND1", "RB1", "CDKN2A", "CCNE1"],
    #     # "Apoptosis": ["BCL2", "BAX", "CASP3", "CASP9", "FAS"],
    # }
    
    # # Run with dry_run=True first to see what would change
    # print("Running in dry run mode to preview changes...")
    # stats = add_category_tags(
    #     '258fd1e7c2e180ba8ea0d909f6b739b3',
    #     os.getenv('NOTION_API_KEY'),
    #     gene_categories,
    #     multiselect_column="protein_gene_sets",
    #     batch_size=10,
    #     dry_run=False  # Set to False to actually make changes
    # )
    
    # # If satisfied with dry run, run again with dry_run=False
    # # stats = add_category_tags(..., dry_run=False)

    # # add_category_tags('258fd1e7c2e180ba8ea0d909f6b739b3', os.getenv('NOTION_API_KEY'), 
    # #                   gene_categories, 'protein_gene_sets',
    # #                   batch_size=10, dry_run=False)