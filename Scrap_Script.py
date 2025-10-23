import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from pymongo import MongoClient
from tqdm.asyncio import tqdm
import platform
import re

# --- 1. CONFIGURATION: Set up the environment and goals ---
# Base URL for the Africa Energy Portal
BASE_URL = "https://africa-energy-portal.org"
# URL to scrape the list of all countries
COUNTRY_LIST_URL = f"{BASE_URL}/country-profile"

# MongoDB connection string 
MONGO_URI = "mongodb://localhost:27017/" 
# Define the target years for extraction
YEARS = [str(y) for y in range(2000, 2025)] 

# --- 2. DATA STORAGE FUNCTION: Connect to MongoDB ---
def setup_mongodb():
    """Establishes connection to the MongoDB database and returns the collection."""
    try:
        client = MongoClient(MONGO_URI)
        # Use a descriptive database and collection name
        collection = client["EnergyDataDB"]["AfricaEnergyMetrics_2000_2024"]
        print("✅ MongoDB connection established.")
        return collection
    except Exception as e:
        print(f"❌ ERROR connecting to MongoDB: {e}")
        # Return None or raise an error to stop execution if necessary
        return None 


# --- 3A. STAGE 1: Get ALL Country URLs Dynamically ---
async def get_all_country_links(p):
    """Scrapes the /country-profile page to get a list of all country names and their profile URLs."""
    
    countries_list = []
    print("🚀 Stage 1: Finding all country profile links...")
    
    # ⚠️ CHANGE 1: Set headless=False to use a visible browser window
    browser = await p.chromium.launch(headless=False, slow_mo=50) 
    page = await browser.new_page()
    
    # Use the more lenient wait condition and long timeout
    await page.goto(COUNTRY_LIST_URL, wait_until="domcontentloaded", timeout=120000)
    
    # ⚠️ CHANGE 2: Wait explicitly for the page to render all JavaScript/content
    print("⏳ Pausing for 8 seconds to allow full JavaScript rendering...")
    await page.wait_for_timeout(8000) # Wait 8 seconds
    
    # CRITICAL SELECTOR: Use the most robust selector we found
    FINAL_LINK_SELECTOR = "a[href^='/aep/country/']"
    
    try:
        # Wait up to 30 seconds for at least ONE country link to appear in the DOM
        print("🔍 Waiting for country list to attach to DOM...")
        await page.wait_for_selector(FINAL_LINK_SELECTOR, state='attached', timeout=30000)
    except Exception as e:
        # This block will still be hit if the selector is never found, even after 8s delay
        print(f"❌ Failed to find country links after waiting: {e}")
        await browser.close()
        return []

    # Once we know the elements are attached, we locate all of them
    country_links_locator = page.locator(FINAL_LINK_SELECTOR)
    country_links_count = await country_links_locator.count()
    
    if country_links_count == 0:
        print("❌ Could not find any country links, even after aggressive waiting.")
        await browser.close()
        return []

    print(f"✅ Found {country_links_count} country links. Extracting details...")
    
    for i in range(country_links_count):
        link = country_links_locator.nth(i)
        href = await link.get_attribute("href")
        name = await link.inner_text()
        
        # Construct the full URL and filter out empty names
        if href and name.strip():
            countries_list.append({
                "name": name.strip(),
                "url": f"{BASE_URL}{href}"
            })

    await browser.close()
    print(f"🌍 Final count of usable country profiles: {len(countries_list)}")
    return countries_list


# --- 3B. STAGE 2: Extract Data for a Single Country ---
async def extract_country_data(browser, country_info, all_data):
    """
    Navigates to a specific country profile URL, scrapes the energy data table,
    and appends the structured data to the shared list 'all_data'.
    """
    country_name = country_info["name"]
    profile_url = country_info["url"]
    
    # Create a new isolated browser context/page for this task
    context = await browser.new_context()
    page = await context.new_page()
    
    try:
        # Navigate to the country-specific profile
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
        
        # --- Data Identification: Locate the main dataset table ---
        # Selector for the table container: div.energy-data-table
        table_selector = 'div.energy-data-table'
        
        # Wait for the table to appear on the page
        await page.wait_for_selector(table_selector, timeout=20000)

        # --- Row Processing: Extract the table data ---
        
        # Get all rows (excluding header tr) from the table
        metric_rows_locator = page.locator(f'{table_selector} table tr:not(:first-child)')
        metric_rows_count = await metric_rows_locator.count()
        
        # Extract the country serial (the ID at the end of the URL, e.g., 20)
        match = re.search(r'/(\d+)$', profile_url)
        country_serial = match.group(1) if match else "NA"
        
        for i in range(metric_rows_count):
            row_locator = metric_rows_locator.nth(i)
            # Get all text content from the row's <td> cells
            cells = await row_locator.locator('td').all_text_contents()
            
            # Expected structure: [Metric Name, Unit, 2000, ..., 2024, Source] (28 cells total)
            if len(cells) >= len(YEARS) + 3: 
                
                # Extract fixed text fields
                metric_name = cells[0].strip()
                unit = cells[1].strip()
                source_name = cells[-1].strip() # Last cell is the source
                
                # Extract year values (starts at index 2, ends before the last cell)
                year_values = []
                for value_str in cells[2:-1]:
                    value_str_clean = value_str.strip().replace(',', '')
                    try:
                        # Convert to float for numeric storage
                        year_values.append(float(value_str_clean))
                    except ValueError:
                        # Store None for missing/NA data
                        year_values.append(None) 

                # Create the structured data entry adhering to the required schema
                record = {
                    "country": country_name,
                    "country_serial": country_serial,
                    "metric": metric_name,
                    "unit": unit,
                    # Sector fields are not available per row on the website table, set to None
                    "sector": None, 
                    "sub_sector": None,
                    "sub_sub_sector": None,
                    "source_link": profile_url,
                    "source": source_name,
                }
                
                # Add the year values to the record
                for year, value in zip(YEARS, year_values):
                    record[year] = value

                all_data.append(record)
            
            # else: Row is incomplete, do nothing (i.e., skip)
        
    except Exception as e:
        print(f"!!! ERROR during extraction for {country_name} ({profile_url}): {e}")
        
    finally:
        await context.close()



# --- 4. MAIN ASYNC EXECUTION ---
async def main_async():
    """Main asynchronous function to orchestrate the data extraction and storage."""
    
    # FIX: Set the event loop policy on Windows for subprocess compatibility (Playwright driver)
    if platform.system() == "Windows":
        # This resolves the NotImplementedError when launching the browser process
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        print("✅ Windows Selector Event Loop Policy set for Playwright.")
        
    collection = setup_mongodb()
    if collection is None:
        return # Exit if MongoDB connection failed
        
    all_extracted_data = []

    # Use the async_playwright context manager
    async with async_playwright() as p:
        
        # Stage 1: Get the list of countries
        countries_to_scrape = await get_all_country_links(p)
        
        if not countries_to_scrape:
            print("❌ Exiting: No countries found to scrape.")
            return
            
        # Launch the main browser for parallel scraping
        # Use a high concurrency limit (e.g., 8-10) for faster scraping
        browser = await p.chromium.launch(headless=False) 
        print("Playwright main browser launched for concurrent scraping.")

        # Stage 2: Create tasks for each country and run concurrently
        tasks = [extract_country_data(browser, info, all_extracted_data) for info in countries_to_scrape]
        
        print("\n⚙️ Stage 2: Starting concurrent data extraction (This may take several minutes)...")
        # Use tqdm.asyncio for a progress bar
        await tqdm.gather(*tasks, desc="Overall Progress")

        await browser.close()
        print("\nPlaywright browser closed.")

        # FINAL CHECK AND STORAGE
        if all_extracted_data:
            print(f"\n📝 Extracted {len(all_extracted_data)} metrics in total.")
            # Insert the list of dictionaries into MongoDB
            # Using ordered=False allows insert to continue even if one document fails
            collection.insert_many(all_extracted_data, ordered=False) 
            print(f"\n✅ SUCCESS! Stored {len(all_extracted_data)} metrics in MongoDB.")
        else:
            print("\n❌ FAILED: No data was extracted for any country.")

# --- 5. ENTRY POINT ---
if __name__ == "__main__":
    # Ensure nest_asyncio is applied for Jupyter/IPython environment compatibility
    try:
        import nest_asyncio
        nest_asyncio.apply()
    except ImportError:
        pass # Not running in an environment that requires nest_asyncio

    # Run the main asynchronous function
    try:
        asyncio.run(main_async())
    except Exception as e:
        print(f"An unexpected error occurred during execution: {e}")