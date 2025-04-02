import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
import os
import pandas as pd

# Function to read a sheet
def read_sheet(_input_file, _sheet_index, _sheet_name, _sheet_names):
    _xls = pd.ExcelFile(_input_file)

    # print(f"Reading sheet: {sheet_name}...")
    _percentage = (_sheet_index + 1) / len(_sheet_names) * 100
    print(f"Reading sheet: {_sheet_name}... {_percentage:.2f}%", flush=True)

    # Read the sheet, nrows = No of row to read, None = read all
    return _sheet_name, pd.read_excel(_xls, sheet_name=_sheet_name, header=0, nrows=None)

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
    # Get number of CPU cores
    num_cpu = os.cpu_count()
    print(f"Number of CPU cores: {num_cpu}")

    # Start time
    start_time = time.time()

    # Load the data from the Excel file
    INPUT_FILE = 'C:/Users/dorem/Downloads/bank.xlsx' 
    INPUT_FILE = INPUT_FILE.strip('\u202a')
    OUTPUT_FILE = 'loai1output_file.xlsx'

    # Load the Excel file
    xls = pd.ExcelFile(INPUT_FILE)
    # Get the sheet names
    # sheet_names = xls.sheet_names[0:4] # Get the first 3 sheets to load faster
    sheet_names = xls.sheet_names
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
    company_headers = [headers[0]]

    general_headers = headers[0:1]
    general_headers.append("Year")
    general_headers.extend(sheet_names)

    # Get the number of years
    years = headers[2:]
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

    # Save DataFrame to a CSV file
    # df.to_csv('output_file.csv', index=False)

    # Alternatively, to save as an Excel file (the xlsxwriter engine is faster)
    df.to_excel(OUTPUT_FILE, index=False, engine = 'xlsxwriter')

    print(df)

    # End time
    end_time = time.time()

    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print(f"Total processing time: {elapsed_time:.2f} seconds")
