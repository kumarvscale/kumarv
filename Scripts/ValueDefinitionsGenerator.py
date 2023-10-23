# %%
import pandas as pd
import requests
from tqdm import tqdm

# %%
#read csv file into a dataframe
df = pd.read_csv('woodpeckervalues.csv',encoding = "ISO-8859-1")
#print df columns
#print(df.columns)

# %%
#create a new df called odf with columns ProductType, attribute, value, and definition
odf = pd.DataFrame(columns=['ProductType', 'attribute', 'value', 'definition'])
   
#iterate through each row of df
for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    #get the value of the first row in 'Product Category' column in variable called ProductType and put it within single quotes
    ProductType = "'" + row['Product Category'] + "'"    
    #get the value of the first row in 'Attribute' column in variable called attribute and put it within single quotes
    attribute = "'" + row['Attribute'] + "'"
    #get the value of the first row in 'Attribute Values' column in variable called value and put it within single quotes
    value = "'" + row['Attribute Values'] + "'"
    #print the value of ProductType, attribute, and value
    #print(ProductType, attribute, value)
    data = {
    "input": {
        "Value": value,
        "Attribute": attribute,
        "Producttype": ProductType
    }
    }
    headers = {"Authorization":"Basic clfv6zfnd011k1arec1lucb9l"}
    response = requests.post(
    "https://dashboard.scale.com/spellbook/api/v2/deploy/kf83pfk",
    json=data,
    headers=headers
    )
    #save the value of output key from response json in variable called definition
    definition = response.json()['output']
    #print(definition)
    #remove all newlines from definition
    definition = definition.replace('\n', '')
    #append ProductType, attribute, value, and definition to odf
    new_df = pd.DataFrame([[ProductType, attribute, value, definition]], columns=['ProductType', 'attribute', 'value', 'definition'], index=[0])
    odf = pd.concat([odf, new_df], ignore_index=True)      
#save odf as a csv file
odf.to_csv('woodpeckerdefinitions.csv', index=False) 

# %%
#save odf to csv
odf.to_csv('woodpeckerdefinitions2.csv', index=False)

# %%



