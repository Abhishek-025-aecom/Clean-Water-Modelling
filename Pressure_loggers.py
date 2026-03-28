import pandas as pd
import glob
import os
from datetime import datetime, timedelta
from collections import Counter


removal_proforma_file_location =  input("Enter the removal_proforma_file_location: ")

surveyed_coordinates_file_location = input("Enter the surveyed_coordinates_file_location: ")

removal_proforma_db = pd.read_excel(removal_proforma_file_location, 
                          sheet_name= 0,
                          header= 0,
                          usecols= [0, 1 , 2, 4, 5, 6, 7, 8, 9 ])

surveyed_coordinates_db = pd.read_excel(surveyed_coordinates_file_location, 
                          sheet_name= 0,
                          header= 0,
                        )

cleansed_data = removal_proforma_db[
    (removal_proforma_db.iloc[:,1] != "NO ALT" ) &
    ((removal_proforma_db.iloc[:,1]).str.strip() != "DUPLICATE")
].copy()

all_loggers_and_loggers_ref = cleansed_data.set_index(cleansed_data.columns[1])[cleansed_data.columns[0]].to_dict()

logger_list1 = cleansed_data.iloc[:,0].tolist()

logger_list2 = surveyed_coordinates_db.iloc[:,3].tolist()

primayer_csv_folder = input("Enter the primayer_csv_folder_location: ")

primayer_loggerfile_list_extension = glob.glob(os.path.join(primayer_csv_folder, "*.csv"))
primayer_loggerfile_list = [os.path.splitext(os.path.basename(file))[0] for file in primayer_loggerfile_list_extension]

loggers_dict = {}
for item in primayer_loggerfile_list:
    ref_part = int(item.split(".")[-1])
    if ref_part in all_loggers_and_loggers_ref:
        loggers_dict[item] = all_loggers_and_loggers_ref[ref_part]

if primayer_csv_folder:
    output_file = "Master_Pressure_Data_2.xlsx"
    # How many rows to skip at the beginning of each CSV (header info)
    # Adjust this based on where your actual data starts
    skip_rows = 0  # Change this number based on your CSVs
    # Column names in your CSVs (adjust if different)
    timestamp_col = 'Datatime'  # or 'Time', 'Date Time', etc.
    pressure_col = 'Pressure A1 15 Minute Average'     # or 'Value', 'Reading', etc.

    print(f"📁 Found {len(primayer_loggerfile_list_extension)} CSV files")
    print("-" * 50)

    # =====================================================
    # STEP 3: Read and Process Each CSV
    # =====================================================

    all_data = []  # Store all dataframes here

    for i, csv_file in enumerate(primayer_loggerfile_list_extension, 1):
        try:
            # Extract logger name from filename
            # Example: "Logger_A.csv" → "Logger_A"
            logger_name = os.path.splitext(os.path.basename(csv_file))[0]
            
            print(f"📊 Processing {i}/{len(primayer_loggerfile_list_extension)}: {logger_name}")
            
            # Read CSV, skipping header rows
            df = pd.read_csv(csv_file, skiprows=skip_rows)
            
            # If your CSV has different column names, rename them
            # Find columns (assuming first two data columns are timestamp & pressure)
            df.columns = [timestamp_col, pressure_col] + list(df.columns[2:])
            
            # Keep only timestamp and pressure columns
            df = df[[timestamp_col, pressure_col]].copy()
            
            # Convert timestamp to datetime
            # Convert timestamp to datetime
            # Try multiple formats
            # First, check if timestamps are numeric (Excel serial format)
            if pd.api.types.is_numeric_dtype(df[timestamp_col]):
                # Convert Excel serial date to datetime
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='D', origin='1899-12-30')
                print(f"   ├─ Timestamp format: Excel serial date")
            else:
                # Try standard string formats
                for fmt in ['%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M']:
                    try:
                        df[timestamp_col] = pd.to_datetime(df[timestamp_col], format=fmt)
                        print(f"   ├─ Timestamp format: {fmt}")
                        break
                    except:
                        continue
                else:
                    # If no format worked, let pandas infer
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
                    print(f"   ├─ Timestamp format: Auto-detected")
            
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

    print(f"\n💾 Saving to Excel: {output_file}")

    # Take user input for main timestamp
    user_ts = input("Enter main timestamp (MM/DD/YYYY HH:MM): ")
    user_ts = pd.to_datetime(user_ts)

    # Find the position of this timestamp and go back 25 rows
    idx = master_df.index.get_loc(user_ts)
    start_idx = max(0, idx - 30)

    # Trim the dataframe
    master_df = master_df.iloc[start_idx:]

    print(f"DataFrame now starts from: {master_df.index[0]}")

    # Filter columns that exist in dict
    cols_to_keep = [col for col in master_df.columns if col in loggers_dict]

    # Select only those columns
    df_filtered = master_df[cols_to_keep]

    # Rename using the dictionary
    master_df = df_filtered.rename(columns=loggers_dict)
    print("\n🔧 Formatting output...")

    # 1. Sort columns in ascending order
    sorted_columns = sorted(master_df.columns)
    master_df = master_df[sorted_columns]
    print(f"   ├─ Columns sorted alphabetically")

    # 2. Reset index to make timestamp a column
    master_df_export = master_df.reset_index()

    # 3. Rename index column to 'Datetime'
    master_df_export = master_df_export.rename(columns={'index': 'Datetime'})
    print(f"   ├─ Index column renamed to 'Datetime'")

    # 4. Format datetime column as dd-mm-yyyy hh:mm:ss
    master_df_export['Datetime'] = master_df_export['Datetime'].dt.strftime('%d-%m-%Y %H:%M:%S')
    print(f"   └─ Datetime formatted as dd-mm-yyyy hh:mm:ss")

    # =====================================================
    # STEP 6: Export to Excel
    # =====================================================

    print(f"\n💾 Saving to Excel: {output_file}")

    # Write to Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Main data sheet
        master_df_export.to_excel(writer, sheet_name='Master_Data', index=False)

    print("✅ DONE! Excel file created successfully!")
    print(f"\n📊 Output file: {output_file}")
    print(f"   ├─ Sheet 1: Master_Data (all aligned data)")

