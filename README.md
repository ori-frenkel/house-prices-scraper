# Haifa Real Estate Data Scraper

This project scrapes and analyzes real estate transaction data from Haifa neighborhoods using the Israeli government's real estate database ([nadlan.gov.il](https://www.nadlan.gov.il/)).

## Important Note About Data Collection

Due to a limitation on the nadlan.gov.il website, each search can only access up to 100 pages (approximately 1,000 entries) of data. To work around this limitation, this project:
1. Breaks down the data collection by neighborhoods
2. Collects up to 100 pages of data for each neighborhood
3. Uses the combine script (`combine_haifa_data.py`) to merge all neighborhood data into a single comprehensive dataset

This approach ensures we can collect data beyond the 1,000-entry limitation while maintaining data integrity.

## Project Structure

```
houses-price/
├── data/
│   └── gov/
│       ├── Haifa/           # Individual neighborhood CSV files
│       └── haifa_combined.csv   # Combined data from all neighborhoods
├── drivers/
│   └── chromedriver-win64/  # Chrome WebDriver for Selenium
├── checkpoints/             # Checkpoint files for data recovery
├── fetch_nadlan_data_id.py  # Main scraping script
└── combine_haifa_data.py    # Data combination script
```

## Features

- Scrapes real estate transaction data from nadlan.gov.il
- Multi-threaded scraping for better performance (configurable via MAX_WORKERS)
- Handles website's 100-page limitation through neighborhood-based collection
- Checkpoint system for data recovery
- Combines data from multiple neighborhoods
- Handles Hebrew text properly

## Data Fields

The collected data includes the following fields:
- כתובת (Address)
- מ"ר (Square meters)
- תאריך עסקה (Transaction date)
- מחיר (Price)
- גוש/חלקה/תת-חלקה (Block/Parcel)
- סוג נכס (Property type)
- חדרים (Rooms)
- קומה (Floor)
- שנת בנייה (Construction year)
- מחיר למ"ר (Price per sqm)
- קומות במבנה (Total floors)
- שכונה (Neighborhood)

## Requirements

- Python 3.x
- Chrome browser
- Required Python packages:
  ```
  selenium>=4.0.0
  pandas>=1.3.0
  ```

## Installation

1. Clone the repository
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```
3. Ensure Chrome WebDriver is in the correct location (`drivers/chromedriver-win64/chromedriver.exe`)

## Usage

### Scraping Data

1. Configure neighborhoods in `fetch_nadlan_data_id.py`:
   ```python
   NEIGHBORHOOD_IDS = [
       {"id": "XXXXX", "name": "Neighborhood Name"},
       # Add more neighborhoods...
   ]
   MAX_WORKERS = 4  # Configure number of parallel threads
   ```

2. Run the scraper:
   ```bash
   python fetch_nadlan_data_id.py
   ```

### Combining Data

To combine individual neighborhood files into a single dataset:
```bash
python combine_haifa_data.py
```

## Data Recovery

The script creates checkpoints during scraping. If the process is interrupted:
1. The checkpoints are saved in the `checkpoints` directory
2. When restarting, the script will continue from the last checkpoint

## Output

- Individual CSV files are saved in `data/gov/Haifa/`
- Combined data is saved as `data/gov/haifa_combined.csv`
- All files use UTF-8 encoding with BOM (utf-8-sig) for proper Hebrew text handling

## Notes

- Uses Selenium for dynamic content handling
- Implements duplicate detection
- Handles multiple transactions per property 