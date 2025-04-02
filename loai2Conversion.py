
import pandas as pd
import time

# Start time
start_time = time.time()

# Load the data from the Excel file
input_file = 'c:/Users/dorem/Downloads/Code data/loai 2.xlsx'
output_file = 'c:/Users/dorem/Downloads/Code data/pythonConversion.xlsx'

# Read the Excel file
data = pd.read_excel(input_file, header=None)

# Init a new data frame
df = pd.DataFrame()
df = pd.DataFrame(columns=['Firm', 'Name', 'Year', 'CASH - GENERIC', 'COMMON SHAREHOLDERS\' EQUITY', 'COMMON STOCK', 'COST OF GOODS SOLD (EXCL DEP)', 'CURRENT ASSETS - TOTAL'])
    
# Year list (extract from the first column and start from row 3)(remmember counting starts from 0)
years = data.iloc[2:, 0].tolist()
index_year = 0


print(years)

# Variable list (unchange acroos all colunm)(use to extract the company name from the name_raw which contain company name and variable)
variables = ["CASH - GENERIC", "COMMON SHAREHOLDERS' EQUITY", "COMMON STOCK", "COST OF GOODS SOLD (EXCL DEP)", "CURRENT ASSETS - TOTAL"]
index_variable = 0

# Block of new modified data to append
new_block = {
    'Firm': [],
    'Name': [],
    'Year': [],
    'CASH - GENERIC': [],
    'COMMON SHAREHOLDERS\' EQUITY': [],
    'COMMON STOCK': [],
    'COST OF GOODS SOLD (EXCL DEP)': [],
    'CURRENT ASSETS - TOTAL': []
}

# Loop to each colunm and read cell top-down 
for col in range(1, data.shape[1]):
    # Get the name_raw and code_raw from the first and second row
    name_raw = data.iloc[0, col]
    code_raw = data.iloc[1, col]
    # print(name_raw)
    # print(code_raw)
    
    # Skip the col if name_raw or code_raw is empty
    if pd.isnull(name_raw) or pd.isnull(code_raw) or name_raw.strip() == "" or code_raw.strip() == "":
        print("Not valid")
        continue
    
    name = name_raw.split('-')[0].strip()
    code_name = code_raw.split('(')[0].strip()        
    # print(code_name)
    
    
    for row in range(2, data.shape[0]):
        cell_value = data.iloc[row, col]
        
        # if cell_value is not a number (indicate the value is error), write NA
        # if not isinstance(cell_value, (int, float)):
        #     cell_value = ""
    
        if(index_variable == 0):
            # df['Firm'] = code_name
            # df['Name'] = name
            # df['Year'] = years[index_year]
            # print("init company")
            # Create a new row for the DataFrame
            new_block['Firm'].append(code_name)
            new_block['Name'].append(name)
            new_block['Year'].append(years[index_year])
            
        new_block[variables[index_variable]].append(cell_value)
        
        if(index_year >= (len(years)) - 1):
            index_year = 0
        else:
            index_year += 1            


    if(index_variable >= (len(variables)) - 1):
        index_variable = 0

        df = df._append(pd.DataFrame(data=new_block))

        new_block = {
            'Firm': [],
            'Name': [],
            'Year': [],
            'CASH - GENERIC': [],
            'COMMON SHAREHOLDERS\' EQUITY': [],
            'COMMON STOCK': [],
            'COST OF GOODS SOLD (EXCL DEP)': [],
            'CURRENT ASSETS - TOTAL': []
        }
        # print(df)
        # print('reset ' + str(index_variable))
    else:
        index_variable += 1
        # print('next ' + str(index_variable))
    
    # i += 1
    # if i == 5:
    #     break

print(df)
# Save DataFrame to a CSV file
# df.to_csv('output_file.csv', index=False)

# Alternatively, to save as an Excel file
df.to_excel('output_file.xlsx', index=False)

# End time
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time
print(f"Total processing time: {elapsed_time:.2f} seconds")
'''

######--------------------------------------------

import pandas as pd
import concurrent.futures
import time

# Start time
start_time = time.time()

# Load the data from the Excel file
input_file = 'c:/Users/dorem/Downloads/Code data/loai 2.xlsx'
output_file = 'c:/Users/dorem/Downloads/Code data/pythonConversion.xlsx'

# Read the Excel file
data = pd.read_excel(input_file, header=None)

# Initialize DataFrame and other required variables
columns = ['Firm', 'Name', 'Year', 'CASH - GENERIC', 'COMMON SHAREHOLDERS\' EQUITY', 'COMMON STOCK', 
           'COST OF GOODS SOLD (EXCL DEP)', 'CURRENT ASSETS - TOTAL']

# Extract year list starting from row 3 (0-indexed)
years = data.iloc[2:, 0].tolist()

# Variable list
variables = ["CASH - GENERIC", "COMMON SHAREHOLDERS' EQUITY", "COMMON STOCK", 
             "COST OF GOODS SOLD (EXCL DEP)", "CURRENT ASSETS - TOTAL"]

# Function to process a subset of columns
def process_columns(start_col, end_col, data, years, variables):
    partial_df = pd.DataFrame(columns=columns)  # Partial result DataFrame
    index_variable = 0
    index_year = 0

    # Temporary block to store rows for appending
    new_block = {col: [] for col in columns}

    for col in range(start_col, end_col):
        name_raw = data.iloc[0, col]
        code_raw = data.iloc[1, col]

        # Skip if `name_raw` or `code_raw` is invalid
        if pd.isnull(name_raw) or pd.isnull(code_raw) or name_raw.strip() == "" or code_raw.strip() == "":
            print("Not valid")
            continue

        name = name_raw.split('-')[0].strip()
        code_name = code_raw.split('(')[0].strip()

        for row in range(2, data.shape[0]):
            cell_value = data.iloc[row, col]

            if index_variable == 0:
                # Append Firm, Name, and Year for the first variable
                new_block['Firm'].append(code_name)
                new_block['Name'].append(name)
                new_block['Year'].append(years[index_year])

            # Append cell value to the corresponding variable
            new_block[variables[index_variable]].append(cell_value)

            # Cycle through years
            index_year = (index_year + 1) % len(years)

        # When all variables are processed, append the new block to the partial DataFrame
        if index_variable == len(variables) - 1:
            partial_df = partial_df._append(pd.DataFrame(data=new_block), ignore_index=True)

            # Reset new_block and index_variable
            new_block = {col: [] for col in columns}
            index_variable = 0
        else:
            index_variable += 1

    return partial_df


# Number of threads and dividing the columns
num_threads = 8
cols_per_thread = (data.shape[1] - 1) // num_threads
threads = []

# Multithreading using ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    for i in range(num_threads):
        start_col = 1 + i * cols_per_thread
        end_col = 1 + (i + 1) * cols_per_thread if i != num_threads - 1 else data.shape[1]
        threads.append(executor.submit(process_columns, start_col, end_col, data, years, variables))

# Combine all partial DataFrames
final_df = pd.concat([thread.result() for thread in threads], ignore_index=True)

# Save the combined DataFrame to an Excel file
final_df.to_excel(output_file, index=False)

print("Processing completed. Output saved to:", output_file)

# End time
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time
print(f"Total processing time: {elapsed_time:.2f} seconds")
'''