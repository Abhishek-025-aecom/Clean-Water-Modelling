import pandas as pd
import glob
import os
from datetime import datetime, timedelta
import re

# =====================================================
# STEP 1: Load Logger Reference Dictionary
# =====================================================

removal_proforma_file_location = input("Enter the removal_proforma_file_location: ")

removal_proforma_db = pd.read_excel(removal_proforma_file_location, 
                          sheet_name=0,
                          header=0,
                          usecols=[0, 1, 2, 4, 5, 6, 7, 8, 9])

cleansed_data = removal_proforma_db[
    (removal_proforma_db.iloc[:,1] != "NO ALT") &
    ((removal_proforma_db.iloc[:,1]).str.strip() != "DUPLICATE")
].copy()

# Dictionary: {logger_ref: logger_name} e.g., {'2156097-BD': '0705P06', '2156025-BB': '0705P01'}
all_loggers_and_loggers_ref = cleansed_data.set_index(cleansed_data.columns[1])[cleansed_data.columns[0]].to_dict()

print(f"📋 Loaded {len(all_loggers_and_loggers_ref)} logger references from removal proforma")
print(f"Sample references: {list(all_loggers_and_loggers_ref.items())[:3]}")

# =====================================================
# FUNCTION: Match logger reference substring
# =====================================================

def find_logger_name(search_value, logger_dict):
    """
    Search for search_value (e.g., '097-BD') within logger reference keys.
    If found, return the corresponding logger name.
    
    Args:
        search_value: String to search (e.g., '097-BD', '025-BB')
        logger_dict: Dictionary {logger_ref: logger_name}
    
    Returns:
        Logger name if match found, None otherwise
    """
    if pd.isna(search_value) or str(search_value).strip() == '':
        return None
    
    search_str = str(search_value).strip()
    
    # Search for this value in any of the logger ref keys
    for logger_ref, logger_name in logger_dict.items():
        if search_str in str(logger_ref):
            return logger_name
    
    return None

# =====================================================
# STEP 2: Process CSV Files
# =====================================================

csv_folder = input("Enter the cello_csv_folder_location: ")

output_file = "Master_Pressure_Data_1.xlsx"

csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))

print(f"\n📁 Found {len(csv_files)} CSV files")
print("-" * 50)

all_data = []

for i, csv_file in enumerate(csv_files, 1):
    try:
        filename = os.path.splitext(os.path.basename(csv_file))[0]
        
        # =====================================================
        # Read B3 and B12 cell values
        # =====================================================
        
        # Read B3 (row 3, column B = column index 1)
        try:
            cell_b3 = pd.read_csv(csv_file, skiprows=2, nrows=1, header=None).iloc[0, 1]
        except:
            cell_b3 = None
        
        # Read B12 (row 12, column B = column index 1)
        try:
            cell_b12 = pd.read_csv(csv_file, skiprows=11, nrows=1, header=None).iloc[0, 1]
        except:
            cell_b12 = None
        
        # =====================================================
        # Match priority: B3 → B12 → filename
        # =====================================================
        
        matched_logger_name = None
        match_source = None
        
        # Try B3 first
        if cell_b3 is not None:
            matched_logger_name = find_logger_name(cell_b3, all_loggers_and_loggers_ref)
            if matched_logger_name:
                match_source = f"B3='{cell_b3}'"
        
        # Try B12 if B3 didn't match
        if matched_logger_name is None and cell_b12 is not None:
            matched_logger_name = find_logger_name(cell_b12, all_loggers_and_loggers_ref)
            if matched_logger_name:
                match_source = f"B12='{cell_b12}'"
        
        # Try filename if both B3 and B12 didn't match
        if matched_logger_name is None:
            matched_logger_name = find_logger_name(filename, all_loggers_and_loggers_ref)
            if matched_logger_name:
                match_source = f"filename='{filename}'"
        
        # Use matched name or keep filename
        if matched_logger_name:
            logger_name = matched_logger_name
            print(f"📊 Processing {i}/{len(csv_files)}: {filename}")
            print(f"   ├─ Matched: {match_source} → {logger_name}")
        else:
            logger_name = filename
            print(f"📊 Processing {i}/{len(csv_files)}: {filename}")
            print(f"   ├─ No match found (B3='{cell_b3}', B12='{cell_b12}'). Using filename.")
        
        # =====================================================
        # Read CSV data
        # =====================================================
        
        df = pd.read_csv(csv_file, skiprows=11, header=0, usecols=[0, 1])
        
        # Standardize first column name to 'Datatime'
        timestamp_col = 'Datatime'
        df.columns.values[0] = timestamp_col
        
        # Get the second column name (pressure column)
        pressure_col = df.columns[1]
        
        # =====================================================
        # Convert timestamp to datetime
        # =====================================================
        
        if pd.api.types.is_numeric_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='D', origin='1899-12-30')
            print(f"   ├─ Timestamp format: Excel serial date")
        else:
            formats_to_try = [
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M',
                '%d-%m-%Y %H:%M:%S',
                '%d-%m-%Y %H:%M'
            ]
            
            parsed = False
            for fmt in formats_to_try:
                try:
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col], format=fmt)
                    print(f"   ├─ Timestamp format: {fmt}")
                    parsed = True
                    break
                except:
                    continue
            
            if not parsed:
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], dayfirst=True, errors='coerce')
                print(f"   ├─ Timestamp format: Auto-detected (DD/MM/YYYY)")
        
        # Remove rows with invalid timestamps
        df = df.dropna(subset=[timestamp_col])
        
        # Rename pressure column to matched logger name
        df = df.rename(columns={pressure_col: logger_name})
        
        # Set timestamp as index
        df = df.set_index(timestamp_col)
        
        # Show date range
        start_date = df.index.min()
        end_date = df.index.max()
        num_readings = len(df)
        print(f"   ├─ Start: {start_date}")
        print(f"   ├─ End:   {end_date}")
        print(f"   └─ Readings: {num_readings}")
        
        all_data.append(df)
        
    except Exception as e:
        print(f"   ❌ Error processing {filename}: {e}")
        import traceback
        traceback.print_exc()
        continue

print("-" * 50)

# =====================================================
# STEP 3: Merge All Data on Common Timestamps
# =====================================================

print("🔄 Merging all loggers...")

master_df = pd.concat(all_data, axis=1, join='outer')
master_df = master_df.sort_index()

print(f"✅ Merged successfully!")
print(f"   ├─ Total timestamps: {len(master_df)}")
print(f"   ├─ Date range: {master_df.index.min()} to {master_df.index.max()}")
print(f"   └─ Number of loggers: {len(master_df.columns)}")

# =====================================================
# STEP 4: Create complete 15-minute timeline
# =====================================================

print("\n🕐 Creating complete 15-minute timeline...")

full_range = pd.date_range(
    start=master_df.index.min(),
    end=master_df.index.max(),
    freq='15min'
)

master_df = master_df.reindex(full_range)

print(f"✅ Complete timeline created!")
print(f"   └─ Total intervals: {len(master_df)}")

# =====================================================
# STEP 5: Trim from user-specified timestamp
# =====================================================

print("\n✂️ Trimming data...")

user_ts = input("Enter main timestamp (DD/MM/YYYY HH:MM): ")
user_ts = pd.to_datetime(user_ts, dayfirst=True)

try:
    idx = master_df.index.get_loc(user_ts)
    start_idx = max(0, idx - 30)
    master_df = master_df.iloc[start_idx:]
    print(f"✅ DataFrame now starts from: {master_df.index[0]}")
except KeyError:
    print(f"⚠️  Warning: Timestamp {user_ts} not found in data. Using full dataset.")

# =====================================================
# STEP 6: Format for export
# =====================================================

print("\n🔧 Formatting output...")

# Sort columns alphabetically (0705P01, 0705P02, 0706P01, etc.)
sorted_columns = sorted(master_df.columns)
master_df = master_df[sorted_columns]
print(f"   ├─ Columns sorted alphabetically")

# Reset index to make timestamp a column
master_df_export = master_df.reset_index()

# Rename index column to 'Datetime'
master_df_export = master_df_export.rename(columns={'index': 'Datetime'})
print(f"   ├─ Index column renamed to 'Datetime'")

# Format datetime column as dd-mm-yyyy hh:mm:ss
master_df_export['Datetime'] = master_df_export['Datetime'].dt.strftime('%d-%m-%Y %H:%M:%S')
print(f"   └─ Datetime formatted as dd-mm-yyyy hh:mm:ss")

# =====================================================
# STEP 7: Export to Excel
# =====================================================

print(f"\n💾 Saving to Excel: {output_file}")

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    master_df_export.to_excel(writer, sheet_name='Master_Data', index=False)

print("✅ DONE! Excel file created successfully!")
print(f"   ├─ Sheet 1: Master_Data (all aligned data)")
print(f"   ├─ Total rows: {len(master_df_export)}")
print(f"   └─ Total columns: {len(master_df_export.columns)}")
