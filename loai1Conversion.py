import pandas as pd
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os

# from pandas.core.internals.blocks import new_block

# Get number of CPU cores
num_cpu = os.cpu_count()
print(f"Number of CPU cores: {num_cpu}")

# Start time
start_time = time.time()

# Load the data from the Excel file
input_file = 'c:/Users/dorem/Downloads/Code data/loai 1 test.xlsx'
output_file = 'c:/Users/dorem/Downloads/Code data/pythonConversion.xlsx'

# Load the Excel file
xls = pd.ExcelFile(input_file)
# Get the sheet names
sheet_names = xls.sheet_names[0:1] # Get the first 3 sheets to load faster
# sheet_names = xls.sheet_names
index = 0

# print(sheet_names[0]);

# print("Data validation....")
# init_data = pd.read_excel(xls, sheet_name=sheet_names[0])

# Initialize a dictionary to hold data for each sheet
sheets_data = {}


# # Read all sheets into memory
for sheet_index, sheet_name in enumerate(sheet_names):
    # print(sheet_name, end=", ") 
    print(f"Reading sheet: {sheet_name}... {100*(sheet_index+1)/len(sheet_names):.2f}%")
    # nrows to load a specific number of rows to reduce loading time!!!!!!!!!!!!!!!!!!!!!!!!!!
    sheets_data[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name, header=0)
    
'''    
# print(sheets_data[sheet_names[0]])
# # End time
# end_time = time.time()

# # Calculate elapsed time
# elapsed_time = end_time - start_time
# print(f"Total processing time: {elapsed_time:.2f} seconds")
# sys.exit(0)
'''

# Intitalize no of rows (All sheets must have same number of rows)
no_of_rows = sheets_data[sheet_names[0]].shape[0]

# Get the headers all the sheets are the same 
# Since we get the header so it will count 0 from the next row
headers = sheets_data[sheet_names[0]].columns.tolist()

# Extract the first 4 col in the headers, which are the company headers (not taking the No)
company_headers = headers[1:5]

general_headers = headers[1:5]
general_headers.append("Year")
general_headers.extend(sheet_names)

# Get the number of years
years = headers[5:]
print(f"Years: {years}")
print(f"General header: {general_headers}")

# Init the block of data to append
new_block = {
    header: [] for header in general_headers
}

# Init a new data frame
df = pd.DataFrame()
###
for row in range(0, no_of_rows):
    # New company init company headers data only once, since each row is a new company
    new_company = True
    for sheet_index, sheet_name in enumerate(sheet_names):
        # print(sheet_name)
        # Get the data in that row
        # print(row)
        data = sheets_data[sheet_name].iloc[row, :]    
        for year in years:
            if new_company:
                new_block["Year"].append(year)
                for co_header in company_headers:
                    # print(co_header)
                    new_block[co_header].append(data[co_header]) # Append the value to the corresponding key
            new_block[sheet_name].append(data[year])
        
        new_company = False
        

        # break
    # print(new_block)  # Print the current state of new_block for verification
        
    df = df._append(pd.DataFrame(data=new_block), ignore_index=True)
    
    # Reset the new_block
    new_block = {
        header: [] for header in general_headers
    }  
    # break
print(df)


# Save DataFrame to a CSV file
# df.to_csv('output_file.csv', index=False)

# Alternatively, to save as an Excel file
df.to_excel('loai1output_file.xlsx', index=False)    
    
# End time
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time
print(f"Total processing time: {elapsed_time:.2f} seconds")