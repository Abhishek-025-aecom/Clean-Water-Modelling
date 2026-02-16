import pandas as pd
import glob
import os
from datetime import datetime, timedelta

# =====================================================
# STEP 1: Configure Your Settings
# =====================================================

# Folder containing all your CSV files
csv_folder = input("Enter the folder location:\n")

# Output Excel file
output_file = "Master_Pressure_Data.xlsx"

# CSV Structure Information:
# - Row 12 (index 11): Contains column headers
# - Row 13 (index 12): Useless row - will be skipped
# - Row 14 (index 13) onwards: Actual data
# - Column A (index 0): Timestamp
# - Column B (index 1): Pressure data

# Dictionary to track filename-header mismatches
mismatch_dict = {}

# Dictionary to track duplicate timestamps
duplicate_info = {}

# =====================================================
# STEP 2: Find All CSV Files
# =====================================================

# Get list of all CSV files in folder
csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))

print(f"\n📁 Found {len(csv_files)} CSV files")
print("-" * 50)

# =====================================================
# STEP 3: Read and Process Each CSV
# =====================================================

all_data = []  # Store all dataframes here

for i, csv_file in enumerate(csv_files, 1):
    try:
        # Extract filename without extension - THIS WILL BE THE LOGGER NAME
        filename_without_ext = os.path.splitext(os.path.basename(csv_file))[0]
        logger_name = filename_without_ext  # Use filename as logger name
        
        print(f"📊 Processing {i}/{len(csv_files)}: {logger_name}")
        
        # First, peek at row 12 to check the column B header (for validation only)
        header_row = pd.read_csv(csv_file, skiprows=11, nrows=1, header=None)
        
        # Get column A and B headers
        if len(header_row.columns) < 2:
            raise ValueError(f"CSV doesn't have at least 2 columns in row 12")
        
        column_a_header = str(header_row.iloc[0, 0]).strip()  # Column A header
        column_b_header = str(header_row.iloc[0, 1]).strip()  # Column B header
        
        print(f"   ├─ Column A header: {column_a_header}")
        print(f"   ├─ Column B header: {column_b_header}")
        
        # Check if filename matches the column B header (for quality control)
        if logger_name != column_b_header:
            print(f"   ⚠️  MISMATCH DETECTED!")
            print(f"   ├─ Filename: {logger_name}")
            print(f"   ├─ Column B Header: {column_b_header}")
            print(f"   └─ Using FILENAME as logger name")
            mismatch_dict[logger_name] = column_b_header
        else:
            print(f"   ✅ Filename matches Column B header")
        
        # Now read the full CSV with proper structure
        df = pd.read_csv(
            csv_file, 
            header=11,        # Row 12 as header (0-indexed: row 12 = index 11)
            skiprows=[12]     # Skip row 13 (0-indexed: row 13 = index 12)
        )
        
        # IMPORTANT: Use ONLY Column A and Column B by position (not by name)
        # Column A (index 0) = Timestamp
        # Column B (index 1) = Pressure
        
        if len(df.columns) < 2:
            raise ValueError(f"CSV doesn't have at least 2 columns")
        
        # Extract ONLY columns A and B by position
        timestamp_col_data = df.iloc[:, 0]  # Column A (first column)
        pressure_col_data = df.iloc[:, 1]   # Column B (second column)
        
        # Create a new dataframe with just these two columns
        df = pd.DataFrame({
            'Timestamp': timestamp_col_data,
            'Pressure': pressure_col_data
        })
        
        print(f"   ├─ Using Column A (position 0) for timestamp")
        print(f"   └─ Using Column B (position 1) for pressure")
        
        # Convert timestamp to datetime
        # Try multiple formats
        for fmt in ['%d/%m/%Y %H:%M', '%m/%d/%Y %H:%M', '%Y-%m-%d %H:%M:%S', '%d-%m-%Y %H:%M', '%d/%m/%Y %H:%M:%S']:
            try:
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=fmt)
                print(f"   ├─ Timestamp format: {fmt}")
                break
            except:
                continue
        else:
            # If no format worked, let pandas infer
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            print(f"   ├─ Timestamp format: Auto-detected")
        
        # Remove any rows with invalid timestamps
        df = df.dropna(subset=['Timestamp'])
        
        # Rename pressure column to logger name (FROM FILENAME)
        df = df.rename(columns={'Pressure': logger_name})
        
        # Set timestamp as index
        df = df.set_index('Timestamp')
        
        # *** FIX FOR DUPLICATE TIMESTAMPS ***
        # Check for duplicate timestamps
        duplicates_count = df.index.duplicated().sum()
        if duplicates_count > 0:
            print(f"   ⚠️  Found {duplicates_count} duplicate timestamps!")
            print(f"   ├─ Handling duplicates: Keeping FIRST occurrence")
            duplicate_info[logger_name] = duplicates_count
            
            # Remove duplicate timestamps (keep first occurrence)
            df = df[~df.index.duplicated(keep='first')]
            print(f"   └─ After removing duplicates: {len(df)} readings")
        
        # Show date range for this logger
        start_date = df.index.min()
        end_date = df.index.max()
        num_readings = len(df)
        print(f"   ├─ Logger name used: {logger_name} (from filename)")
        print(f"   ├─ Start: {start_date}")
        print(f"   ├─ End:   {end_date}")
        print(f"   └─ Readings: {num_readings}")
        
        all_data.append(df)
        
    except Exception as e:
        print(f"   ❌ Error processing {logger_name}: {e}")
        import traceback
        print(f"   └─ Details: {traceback.format_exc()}")
        continue

print("-" * 50)

# =====================================================
# STEP 3A: Report Filename-Header Mismatches
# =====================================================

if len(mismatch_dict) > 0:
    print("\n" + "=" * 60)
    print("⚠️  FILENAME vs COLUMN B HEADER MISMATCH REPORT")
    print("=" * 60)
    print(f"Found {len(mismatch_dict)} file(s) where filename doesn't match Column B header:")
    print("-" * 60)
    print(f"{'Filename (Logger Name Used)':<35} | {'Column B Header':<30}")
    print("-" * 60)
    for filename, header in mismatch_dict.items():
        print(f"{filename:<35} | {header:<30}")
    print("-" * 60)
    print("Note: Logger names in Excel use FILENAMES, not column headers.")
    print("This is a quality control flag - check if headers need updating.")
    print("=" * 60 + "\n")
else:
    print("\n✅ All filenames match their Column B headers perfectly!")
    print("-" * 50 + "\n")

# =====================================================
# STEP 3B: Report Duplicate Timestamps
# =====================================================

if len(duplicate_info) > 0:
    print("\n" + "=" * 60)
    print("⚠️  DUPLICATE TIMESTAMPS REPORT")
    print("=" * 60)
    print(f"Found duplicate timestamps in {len(duplicate_info)} file(s):")
    print("-" * 60)
    print(f"{'Logger Name':<30} | {'Duplicates Removed':<20}")
    print("-" * 60)
    for logger, count in duplicate_info.items():
        print(f"{logger:<30} | {count:<20}")
    print("-" * 60)
    print("Action taken: Kept FIRST occurrence, removed subsequent duplicates.")
    print("=" * 60 + "\n")
else:
    print("\n✅ No duplicate timestamps found in any files!")
    print("-" * 50 + "\n")

# Check if any data was successfully loaded
if len(all_data) == 0:
    print("❌ ERROR: No data was successfully loaded from any CSV file!")
    print("Please check:")
    print("  - CSV file structure (headers in row 12?)")
    print("  - Column A has timestamp data")
    print("  - Column B has pressure data")
    print("  - CSV files contain valid data")
    exit()

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
# STEP 5: Fill Missing 15-min Intervals
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
# STEP 6: Get User Input for Timestamp Filtering
# =====================================================

print("\n" + "=" * 60)
print("TIMESTAMP FILTERING")
print("=" * 60)
print("Enter the reference timestamp.")
print("The data will start from 25 time steps (6h 15m) BEFORE this time.")
print("\nFormat: DD/MM/YYYY HH:MM or MM/DD/YYYY HH:MM")
print("Example: 12/12/2022 23:15")
print("-" * 60)

while True:
    user_input = input("Enter reference timestamp: ").strip()
    
    try:
        # Try multiple formats
        reference_time = None
        for fmt in ['%d/%m/%Y %H:%M', '%m/%d/%Y %H:%M', '%Y-%m-%d %H:%M']:
            try:
                reference_time = pd.to_datetime(user_input, format=fmt)
                break
            except:
                continue
        
        if reference_time is None:
            # Let pandas try to parse it
            reference_time = pd.to_datetime(user_input)
        
        # Calculate 25 time steps back (25 * 15 minutes = 375 minutes)
        time_steps_back = 25
        minutes_back = time_steps_back * 15
        min_time_cutoff = reference_time - timedelta(minutes=minutes_back)
        
        print("\n✅ Valid timestamp entered!")
        print(f"   Reference time:  {reference_time.strftime('%d/%m/%Y %H:%M')}")
        print(f"   Time steps back: {time_steps_back} (= {minutes_back} minutes = {minutes_back//60}h {minutes_back%60}m)")
        print(f"   Minimum cutoff:  {min_time_cutoff.strftime('%d/%m/%Y %H:%M')}")
        print(f"\n   Data will START from: {min_time_cutoff.strftime('%d/%m/%Y %H:%M')}")
        
        # Check if cutoff is within data range
        if min_time_cutoff < master_df.index.min():
            print(f"\n   ⚠️  Note: Cutoff time is before data starts.")
            print(f"   Data actually starts at: {master_df.index.min()}")
            print(f"   Will use earliest available data instead.")
        
        if reference_time > master_df.index.max():
            print(f"\n   ⚠️  Warning: Reference time is after data ends!")
            print(f"   Data ends at: {master_df.index.max()}")
        
        # Confirm with user
        confirm = input("\nIs this correct? (yes/no): ").strip().lower()
        if confirm in ['yes', 'y']:
            break
        else:
            print("\nLet's try again...\n")
            
    except Exception as e:
        print(f"\n❌ Invalid format! Error: {e}")
        print("Please use format: DD/MM/YYYY HH:MM (e.g., 12/12/2022 23:15)\n")

print("\n" + "=" * 60)

# =====================================================
# STEP 7: Filter Data Based on Cutoff Time
# =====================================================

print(f"\n✂️  Filtering data from {min_time_cutoff.strftime('%d/%m/%Y %H:%M')} onwards...")

rows_before = len(master_df)
master_df = master_df[master_df.index >= min_time_cutoff]
rows_after = len(master_df)
rows_removed = rows_before - rows_after

print(f"✅ Filtering complete!")
print(f"   ├─ Rows before: {rows_before}")
print(f"   ├─ Rows after:  {rows_after}")
print(f"   ├─ Rows removed: {rows_removed}")
print(f"   └─ New date range: {master_df.index.min()} to {master_df.index.max()}")

if len(master_df) == 0:
    print("\n⚠️  WARNING: No data remains after filtering!")
    print(f"   The minimum cutoff time is after all available data.")
    exit()

# =====================================================
# STEP 8: Add Statistics Columns
# =====================================================

# Add average pressure across all loggers for each timestamp
master_df['Average_All_Loggers'] = master_df.mean(axis=1)

# Count how many loggers have data at each timestamp
master_df['Active_Loggers'] = master_df.iloc[:, :-1].notna().sum(axis=1)

# =====================================================
# STEP 9: Export to Excel
# =====================================================

print(f"\n💾 Saving to Excel: {output_file}")

# Reset index to make timestamp a column
master_df_export = master_df.reset_index()
master_df_export = master_df_export.rename(columns={'index': 'Datatime'})

# Write to Excel
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    # Main data sheet
    master_df_export.to_excel(writer, sheet_name='Master_Data', index=False)
    
    # Summary sheet
    summary = pd.DataFrame({
        'Logger Name': master_df.columns[:-2],  # Exclude average and count columns
        'First Reading': [master_df[col].first_valid_index() for col in master_df.columns[:-2]],
        'Last Reading': [master_df[col].last_valid_index() for col in master_df.columns[:-2]],
        'Total Readings': [master_df[col].notna().sum() for col in master_df.columns[:-2]],
        'Min Pressure': [master_df[col].min() for col in master_df.columns[:-2]],
        'Max Pressure': [master_df[col].max() for col in master_df.columns[:-2]],
        'Mean Pressure': [master_df[col].mean() for col in master_df.columns[:-2]]
    })
    summary.to_excel(writer, sheet_name='Summary', index=False)
    
    # Filter information sheet
    filter_info = pd.DataFrame({
        'Parameter': ['Reference Timestamp', 'Time Steps Back', 'Minutes Back', 'Minimum Cutoff Time', 
                      'Data Start Date', 'Data End Date', 'Total Rows in Export', 'Rows Filtered Out',
                      'CSV Structure', 'Header Row', 'Data Start Row', 
                      'Column Usage', 'Logger Name Source',
                      'Files with Mismatches', 'Files with Duplicate Timestamps'],
        'Value': [reference_time.strftime('%d/%m/%Y %H:%M'), 
                  time_steps_back, 
                  minutes_back,
                  min_time_cutoff.strftime('%d/%m/%Y %H:%M'),
                  master_df.index.min().strftime('%d/%m/%Y %H:%M'),
                  master_df.index.max().strftime('%d/%m/%Y %H:%M'),
                  rows_after,
                  rows_removed,
                  'Row 12=Headers, Row 13=Skipped, Row 14+=Data',
                  'Row 12',
                  'Row 14',
                  'Column A=Timestamp, Column B=Pressure',
                  'CSV Filename (not column header)',
                  len(mismatch_dict),
                  len(duplicate_info)]
    })
    filter_info.to_excel(writer, sheet_name='Filter_Info', index=False)
    
    # Mismatch report sheet (if any mismatches exist)
    if len(mismatch_dict) > 0:
        mismatch_df = pd.DataFrame({
            'CSV Filename (Logger Name)': list(mismatch_dict.keys()),
            'Column B Header': list(mismatch_dict.values()),
            'Note': ['Quality control flag - filename used as logger name'] * len(mismatch_dict)
        })
        mismatch_df.to_excel(writer, sheet_name='Filename_Mismatches', index=False)
        print(f"   ├─ Sheet 4: Filename_Mismatches ({len(mismatch_dict)} mismatches found)")
    
    # Duplicate timestamps report sheet (if any duplicates exist)
    if len(duplicate_info) > 0:
        duplicate_df = pd.DataFrame({
            'Logger Name': list(duplicate_info.keys()),
            'Duplicates Removed': list(duplicate_info.values()),
            'Action Taken': ['Kept first occurrence'] * len(duplicate_info)
        })
        sheet_num = 5 if len(mismatch_dict) > 0 else 4
        duplicate_df.to_excel(writer, sheet_name='Duplicate_Timestamps', index=False)
        print(f"   ├─ Sheet {sheet_num}: Duplicate_Timestamps ({len(duplicate_info)} files affected)")

print("✅ DONE! Excel file created successfully!")
print(f"\n📊 Output file: {output_file}")
print(f"   ├─ Sheet 1: Master_Data (filtered aligned data)")
print(f"   ├─ Sheet 2: Summary (statistics for each logger)")
print(f"   ├─ Sheet 3: Filter_Info (filtering parameters used)")
if len(mismatch_dict) > 0:
    print(f"   ├─ Sheet 4: Filename_Mismatches (quality control report)")
if len(duplicate_info) > 0:
    sheet_num = 5 if len(mismatch_dict) > 0 else 4
    print(f"   └─ Sheet {sheet_num}: Duplicate_Timestamps (duplicate handling report)")
else:
    if len(mismatch_dict) > 0:
        print(f"   └─ No duplicate timestamps found")
    else:
        print(f"   └─ No mismatches or duplicates detected")
print("\n" + "=" * 60)

# Final summary
print("\n" + "=" * 60)
print("PROCESSING SUMMARY")
print("=" * 60)
print(f"Total CSV files processed: {len(all_data)}")
print(f"Total loggers in output: {len(master_df.columns) - 2}")  # Exclude Average and Active columns
print(f"Column usage: Column A (timestamp), Column B (pressure)")
print(f"Logger names from: CSV filenames")
print(f"Filename vs Column B mismatches: {len(mismatch_dict)}")
print(f"Files with duplicate timestamps: {len(duplicate_info)}")
if len(mismatch_dict) > 0:
    print("\nFiles with filename-header mismatches (quality control):")
    for filename, header in mismatch_dict.items():
        print(f"  • Filename: {filename}.csv | Column B Header: {header}")
        print(f"    → Using '{filename}' as logger name")
if len(duplicate_info) > 0:
    print("\nFiles with duplicates (removed):")
    for logger, count in duplicate_info.items():
        print(f"  • {logger}: {count} duplicate timestamps removed")
print("=" * 60)
