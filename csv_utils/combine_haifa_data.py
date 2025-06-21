import pandas as pd
import os
from pathlib import Path

# paths relative to project root
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_dir = os.path.join(ROOT_DIR, 'data', 'gov', 'Haifa')
output_file = os.path.join(ROOT_DIR, 'data', 'gov', 'haifa_combined.csv')

# empty list to store dataframes
dfs = []
total_records = 0


print(f"Reading files from {input_dir}...")
for file in os.listdir(input_dir):
    if file.endswith('.csv'):
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path)
        records = len(df)
        total_records += records
        
        # add neighborhood name (remove .csv extension)
        neighborhood_name = os.path.splitext(file)[0]
        df['שכונה'] = neighborhood_name
        
        dfs.append(df)
        print(f"Processed {file}: {records} records")

print("\nCombining all dataframes...")
combined_df = pd.concat(dfs, ignore_index=True)

combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"\nResults:")
print(f"- Combined {len(dfs)} files")
print(f"- Total input records: {total_records}")
print(f"- Total output records: {len(combined_df)}")
print(f"- Output file: {output_file}")

# display sample of columns
print(f"\nColumns in combined file:")
for col in combined_df.columns:
    print(f"- {col}") 