import pandas as pd
import glob
import os
from datetime import datetime

# =====================================================
# STEP 1: Configure Your Settings
# =====================================================

# Folder containing all your CSV files
csv_folder = input("Enter the folder location:\n")  # Change this to your folder path

# Output Excel file
output_file = "Master_Pressure_Data.xlsx"

# How many rows to skip at the beginning of each CSV (header info)
# Adjust this based on where your actual data starts
skip_rows = 0  # Change this number based on your CSVs

# Column names in your CSVs (adjust if different)
timestamp_col = 'Datatime'  # or 'Time', 'Date Time', etc.
pressure_col = 'Pressure A1 15 Minute Average'     # or 'Value', 'Reading', etc.

# =====================================================
# STEP 2: Find All CSV Files
# =====================================================

# Get list of all CSV files in folder
csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))

print(f"📁 Found {len(csv_files)} CSV files")
print("-" * 50)

# =====================================================
# STEP 3: Read and Process Each CSV
# =====================================================

all_data = []  # Store all dataframes here

for i, csv_file in enumerate(csv_files, 1):
    try:
        # Extract logger name from filename
        # Example: "Logger_A.csv" → "Logger_A"
        logger_name = os.path.splitext(os.path.basename(csv_file))[0]
        
        print(f"📊 Processing {i}/{len(csv_files)}: {logger_name}")
        
        # Read CSV, skipping header rows
        df = pd.read_csv(csv_file, skiprows=skip_rows)
        
        # If your CSV has different column names, rename them
        # Find columns (assuming first two data columns are timestamp & pressure)
        df.columns = [timestamp_col, pressure_col] + list(df.columns[2:])
        
        # Keep only timestamp and pressure columns
        df = df[[timestamp_col, pressure_col]].copy()
        
        # Convert timestamp to datetime
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], format='%d/%m/%Y %H:%M',errors='coerce')
        
        # Remove any rows with invalid timestamps
        df = df.dropna(subset=[timestamp_col])
        
        # Rename pressure column to logger name
        df = df.rename(columns={pressure_col: logger_name})
        
        # Set timestamp as index
        df = df.set_index(timestamp_col)
        
        # Show date range for this logger
        start_date = df.index.min()
        end_date = df.index.max()
        num_readings = len(df)
        print(f"   ├─ Start: {start_date}")
        print(f"   ├─ End:   {end_date}")
        print(f"   └─ Readings: {num_readings}")
        
        all_data.append(df)
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        continue

print("-" * 50)

# =====================================================
# STEP 4: Merge All Data on Common Timestamps
# =====================================================

print("🔄 Merging all loggers...")

# Combine all dataframes
# outer join = keep all timestamps from all loggers
master_df = pd.concat(all_data, axis=1, join='outer')

# Sort by timestamp
master_df = master_df.sort_index()

print(f"✅ Merged successfully!")
print(f"   ├─ Total timestamps: {len(master_df)}")
print(f"   ├─ Date range: {master_df.index.min()} to {master_df.index.max()}")
print(f"   └─ Number of loggers: {len(master_df.columns)}")

# =====================================================
# STEP 5: Optional - Fill Missing 15-min Intervals
# =====================================================

print("\n🕐 Creating complete 15-minute timeline...")

# Create complete 15-minute interval range
full_range = pd.date_range(
    start=master_df.index.min(),
    end=master_df.index.max(),
    freq='15min'  # 15-minute intervals
)

# Reindex to include all 15-min intervals (fill gaps with NaN)
master_df = master_df.reindex(full_range)

print(f"✅ Complete timeline created!")
print(f"   └─ Total intervals: {len(master_df)}")

# =====================================================
# =====================================================
# STEP 7: Export to Excel
# =====================================================

print(f"\n💾 Saving to Excel: {output_file}")

# Take user input for main timestamp
user_ts = input("Enter main timestamp (MM/DD/YYYY HH:MM): ")
user_ts = pd.to_datetime(user_ts)

# Find the position of this timestamp and go back 25 rows
idx = master_df.index.get_loc(user_ts)
start_idx = max(0, idx - 25)

# Trim the dataframe
master_df = master_df.iloc[start_idx:]

print(f"DataFrame now starts from: {master_df.index[0]}")
# Reset index to make timestamp a column
master_df_export = master_df.reset_index()
master_df_export = master_df_export.rename(columns={'index': 'Datatime'})

# Write to Excel
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    # Main data sheet
    master_df_export.to_excel(writer, sheet_name='Master_Data', index=False)
    
    # Summary sheet
    

print("✅ DONE! Excel file created successfully!")
print(f"\n📊 Output file: {output_file}")
print(f"   ├─ Sheet 1: Master_Data (all aligned data)")
