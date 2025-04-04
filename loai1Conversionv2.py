import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
import os
import pandas as pd
import msvcrt  # For Windows file locking check

# CONST VARIABLES
# Load the data from the Excel file
INPUT_FILE = 'loai 1Short.xlsx' 
INPUT_FILE = INPUT_FILE.strip('\u202a')
OUTPUT_FILE = 'loai1Out.xlsx'

# Set the number of rows to read from each sheet
NO_OF_ROWS = None 

# Set the year columns to read from each sheet (counting from 0)
YEAR_COL_START = 5

# Set the company header range to read from each sheet (counting from 0) 
COM_COL_START = 0
COM_COL_END = 5     # From 0 to 5, not including 5

# Set the Sheet range to read from (counting from 0)
SHEET_START = 0
SHEET_END = None    # None = read all sheets

# Check if the output file is open in another program
def is_file_locked(filepath):
    """Check if a file is locked by another process"""
    if not os.path.exists(filepath):
        return False
    try:
        # Try to open the file in write mode
        with open(filepath, 'ab') as f:
            # Try to get an exclusive lock
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            # If we get here, the file is not locked
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        return False
    except (IOError, PermissionError):
        return True
    
# Function to read a sheet
def read_sheet(_input_file, _sheet_index, _sheet_name, _sheet_names):
    _xls = pd.ExcelFile(_input_file)

    # print(f"Reading sheet: {sheet_name}...")
    _percentage = (_sheet_index + 1) / len(_sheet_names) * 100
    print(f"Reading sheet: {_sheet_name}... {_percentage:.2f}%", flush=True)

    # Read the sheet, nrows = No of row to read, None = read all
    return _sheet_name, pd.read_excel(_xls, sheet_name=_sheet_name, header=0, nrows=NO_OF_ROWS)

def process_rows(_row_range, _sheets_data, _sheet_names, _years, _company_headers, _general_headers):
    _start_row, _end_row = _row_range
    _results = {header: [] for header in _general_headers}

    try:
        for row in range(_start_row, _end_row):
            try:
                for year in _years:
                    row_empty_flag = False
                    
                    try:
                        row_data_header = _sheets_data[_sheet_names[0]].iloc[row, :]
                    except Exception as e:
                        print(f"CRITICAL ERROR at row {row}, sheet {_sheet_names[0]}: {e}")
                        raise  # Re-raise the exception to stop processing

                    for _sheet_name in _sheet_names:
                        try:
                            row_data = _sheets_data[_sheet_name].iloc[row, :]
                            cell_data = row_data[year]

                            if not pd.isna(cell_data):
                                row_empty_flag = False

                            _results[_sheet_name].append(cell_data)
                        except Exception as e:
                            print(f"CRITICAL ERROR at row {row}, sheet {_sheet_name}, year {year}: {e}")
                            raise  # Re-raise the exception to stop processing

                    _results["Year"].append(year)
                    for co_header in _company_headers:
                        try:
                            _results[co_header].append(row_data_header[co_header])
                        except Exception as e:
                            print(f"CRITICAL ERROR at row {row}, header {co_header}: {e}")
                            raise  # Re-raise the exception to stop processing

                    if row_empty_flag:
                        _results["Year"].pop()
                        for co_header in _company_headers:
                            _results[co_header].pop()
                        for _sheet_name in _sheet_names:
                            _results[_sheet_name].pop()

            except Exception as e:
                print(f"CRITICAL ERROR processing row {row}: {e}")
                raise  # Re-raise the exception to stop processing

        return pd.DataFrame(data=_results)
    
    except Exception as e:
        print(f"CRITICAL ERROR in process_rows: {e}")
        import traceback
        traceback.print_exc()  # Print detailed stack trace
        raise  # Re-raise the exception to stop processing


########################## MAIN FUNCTION ############################
if __name__ == "__main__":
    
    # Check if output file is open before proceeding
    if os.path.exists(OUTPUT_FILE) and is_file_locked(OUTPUT_FILE):
        raise IOError(f"ERROR: The output file '{OUTPUT_FILE}' is currently open in another program. "
                    f"Please close it and run the script again.")
        
    # Get number of CPU cores
    num_cpu = os.cpu_count()
    print(f"Number of CPU cores: {num_cpu}")

    # Start time
    start_time = time.time()


    # Load the Excel file
    xls = pd.ExcelFile(INPUT_FILE)
    # Get the sheet names
    if SHEET_END is None:   # If SHEET_END is None, read all sheets
        SHEET_END = len(xls.sheet_names)
    sheet_names = xls.sheet_names[SHEET_START:SHEET_END] # Get the first 3 sheets to load faster
    # sheet_names = xls.sheet_names
    index = 0

    # Initialize a dictionary to hold data for each sheet
    sheets_data = {}

    # Read all sheets into memory using multi-threading
    with ProcessPoolExecutor(max_workers=num_cpu) as executor:
        futures = {executor.submit(read_sheet, INPUT_FILE, sheet_index, sheet_name, sheet_names): sheet_name for sheet_index, sheet_name in enumerate(sheet_names)}
        for future in as_completed(futures):
            index += 1
            sheet_name, data = future.result()
            sheets_data[sheet_name] = data

    print("Done with reading, now processing...")

    # Intitalize no of rows (All sheets must have same number of rows)
    no_of_rows = sheets_data[sheet_names[0]].shape[0]

    # Get the headers all the sheets are the same
    # Since we get the header so it will count 0 from the next row
    headers = sheets_data[sheet_names[0]].columns.tolist()

    # Extract the first 4 col in the headers, which are the company headers (not taking the No)
    company_headers = headers[COM_COL_START:COM_COL_END]
    print(f"Company headers: {company_headers}")
    general_headers = headers[COM_COL_START:COM_COL_END]
    general_headers.append("Year")
    general_headers.extend(sheet_names)

    # Get the number of years
    years = headers[YEAR_COL_START:]
    print(f"Years: {years}")

    # Calculate the number of rows per thread
    rows_per_thread = no_of_rows // num_cpu
    remaining_rows = no_of_rows % num_cpu

    # Create row ranges
    row_ranges = []
    start_row = 0

    for i in range(num_cpu):
        end_row = start_row + rows_per_thread + (1 if i < remaining_rows else 0)  # Distribute remaining rows
        row_ranges.append((start_row, end_row))
        start_row = end_row

    print(f"Row_ranges: {row_ranges}")
    print("Start processing...")

    # Main processing with threading
    with ProcessPoolExecutor(max_workers=num_cpu) as executor:
        # process_row_partial = partial(process_row, sheets_data=sheets_data, sheet_names=sheet_names, years=years, company_headers=company_headers, general_headers=general_headers)
        results = list(executor.map(partial(process_rows, _sheets_data=sheets_data, _sheet_names=sheet_names, _years=years, _company_headers=company_headers, _general_headers=general_headers), row_ranges))

    print("Done with processing, now making dataframe...")

    # Init a new data frame
    df = pd.DataFrame()
    # Concatenate all DataFrames at once
    df = pd.concat(results, ignore_index=True)

    print("Done with making dataframe, now saving...")

    # Handle the Excel row limit by splitting into multiple sheets
    MAX_ROWS_PER_SHEET = 1000000  # Just under Excel's limit for safety
    
    # Create a writer object with xlsxwriter engine
    with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
        # Calculate how many sheets we need
        num_sheets = (len(df) // MAX_ROWS_PER_SHEET) + (1 if len(df) % MAX_ROWS_PER_SHEET else 0)
        
        for i in range(num_sheets):
            start_idx = i * MAX_ROWS_PER_SHEET
            end_idx = min((i + 1) * MAX_ROWS_PER_SHEET, len(df))
            
            sheet_name = f"Data_Part_{i+1}"
            df.iloc[start_idx:end_idx].to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Saved sheet {sheet_name} with {end_idx - start_idx} rows")

    print(f"Data saved to {OUTPUT_FILE} in {num_sheets} sheets")
    
    # Add a statistics sheet showing percentage of blank cells for each column
    print("Generating statistics on blank cells...")
    stats = {}
    
    # Calculate statistics for data columns (sheets)
    for column in sheet_names:
        print(f"Processing column: {column}")
        total_cells = len(df[column])
        blank_cells = df[column].isna().sum()
        percent_blank = (blank_cells / total_cells) * 100 if total_cells > 0 else 0
        print(f"Total Cells: {total_cells}, Blank Cells: {blank_cells}, Percent Blank: {percent_blank:.2f}%")
        stats[column] = {
            'Total Cells': total_cells,
            'Blank Cells': blank_cells,
            'Percent Blank': percent_blank / 100  # Store as decimal for Excel formatting
        }
    
    # Create a DataFrame for the statistics
    stats_df = pd.DataFrame({
        'Column': list(stats.keys()),
        'Total Cells': [stats[col]['Total Cells'] for col in stats],
        'Blank Cells': [stats[col]['Blank Cells'] for col in stats],
        'Percent Blank': [stats[col]['Percent Blank'] for col in stats]
    })
    
    # Append statistics to the Excel file
    with pd.ExcelWriter(OUTPUT_FILE, mode='a', engine='openpyxl') as writer:
        # Don't save the index
        stats_df.to_excel(writer, sheet_name='Blank Cells Statistics', index=False)
        
        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['Blank Cells Statistics']
        
        # Format the Percent Blank column to show as percentage
        # Find the column index for 'Percent Blank'
        percent_col_index = stats_df.columns.get_loc('Percent Blank') + 1  # +1 because Excel is 1-indexed
        
        # Format the percentage column
        for idx in range(2, len(stats_df) + 2):  # Start at row 2 (after header)
            cell = worksheet.cell(row=idx, column=percent_col_index)
            cell.number_format = '0.00%'
    
    print("Statistics sheet added successfully")

    print(df)

    # End time
    end_time = time.time()

    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print(f"Total processing time: {elapsed_time:.2f} seconds")
