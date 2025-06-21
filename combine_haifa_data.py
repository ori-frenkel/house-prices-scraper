import pandas as pd
import os
from pathlib import Path

# Define paths
input_dir = 'data/gov/Haifa'
output_file = 'data/gov/haifa_combined.csv'

# Create an empty list to store dataframes
dfs = []
total_records = 0

# Read each CSV file
print(f"Reading files from {input_dir}...")
for file in os.listdir(input_dir):
    if file.endswith('.csv'):
        # Read the CSV
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path)
        records = len(df)
        total_records += records
        
        # Add neighborhood name (remove .csv extension)
        neighborhood_name = os.path.splitext(file)[0]
        df['שכונה'] = neighborhood_name
        
        # Append to list
        dfs.append(df)
        print(f"Processed {file}: {records} records")

# Combine all dataframes
print("\nCombining all dataframes...")
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined dataframe
combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"\nResults:")
print(f"- Combined {len(dfs)} files")
print(f"- Total input records: {total_records}")
print(f"- Total output records: {len(combined_df)}")
print(f"- Output file: {output_file}")

# Display sample of columns
print(f"\nColumns in combined file:")
for col in combined_df.columns:
    print(f"- {col}") 