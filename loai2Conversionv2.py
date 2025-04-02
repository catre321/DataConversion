import time
import os
from typing import Literal
import pandas as pd

# Start time
start_time = time.time()

# Load the data from the Excel file
COMPANY_NAME = 'c:/Users/dorem/Downloads/Code data/company.xlsx'
INPUT_FILE = 'c:/Users/dorem/Downloads/Code data/loai 2Short.xlsx'
OUTPUT_FILE = 'c:/Users/dorem/Downloads/Code data/pythonConversion.xlsx'



################################## MAIN FUNCTION ##################################
# Get list of company name
company_name_df = pd.read_excel(COMPANY_NAME, header=None)
company_name = company_name_df.iloc[:, 0].tolist()

data_df = pd.read_excel(INPUT_FILE, header=None)

no_of_rows = data_df.shape[0]
no_of_cols = data_df.shape[1]

# Get the year list (the first col from row 3)
years = data_df.iloc[2:, 0].tolist()
index_year = 0

for col in range(1, no_of_cols):
    # Extract the company name and variable from the first and second row
    name_raw: str = data_df.iloc[0, col]
    code_raw: str = data_df.iloc[1, col]
    # print(name_raw)
    # print(code_raw)
    name_raw = name_raw.strip()
    # code_raw = code_raw.strip()
    # Skip the col if name_raw or code_raw is empty
    if pd.isnull(name_raw) or pd.isnull(code_raw) or name_raw.strip() == "" or code_raw.strip() == "":
        print("Not valid")
        continue
    
    name = name_raw.split('-')[0].strip()
    


print(company_name)
os.abort()

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