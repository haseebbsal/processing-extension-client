from flask import Flask, render_template,jsonify,request,send_file
from flask_cors import CORS

import pandas as pd
import numpy as np
import csv
import re
import os

app = Flask(__name__)

CORS(app)




@app.route('/upload', methods=['POST'])
def create_and_save_excel():

    column = request.form['column']
    file = request.files['file']
    filename = file.filename
    file_path = os.path.join("static", filename)
    file.save(file_path)

    df = pd.read_csv(f"static/{filename}")
    os.remove(file_path)

    # Funtion for Number in Case
    # df = pd.read_csv('input.csv')


    # Funtion for Number in Case
    def extract_number(description):
        match = re.search(r'(\d+)-', description)
        if match:
            return int(match.group(1))
        
        # Default Rules as mentioned in the documentation
        if '750' in description:
            return 12  
        elif 'LTR' or 'CS' in description:
            return 9   
        
        return None  


    # Function For Brand
    def find_brands(description):
        brands_list = ''
        csv_file_path = 'static/brands.csv'
        
        with open(csv_file_path, newline='', encoding='iso-8859-1') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                brand_name = row[0].lower()  
                if brand_name in description.lower():
                    brands_list+=(str(brand_name.upper()))
                    break 
        
        return brands_list


    # Function for Drink Type
    def find_drink_types(description):
        found_type = None
        description_lower = description.lower()

        drink_types_csv = 'static/drink_types.csv'

        drink_types_lower = set()
        with open(drink_types_csv, newline='', encoding='iso-8859-1') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                drink_types_lower.add(row[0].lower())

        for drink in drink_types_lower:
            if drink in description_lower:
                found_type = drink.upper()
                break 
        
        # Additional Patterns
        if found_type is None:
            additional_patterns = {
                'mix': 'Mixed Drink',
                'dsrt wht': 'Dessert Wine',
                'chard': 'Chardonnay',
                'champ': 'Champagne',
                'sauv': 'Sauvignon blanc',
                's blanc':'Sauvignon blanc',
                'beers':'Beer',
                'keg': 'Beer',
                '/oz': 'Cider',
                '750': 'Wine',
                '4/5': 'Wine',
                'temp':'Wine',
                'liqeur':'liquor',
                'white claw':'seltzer',
                'wht': 'White Wine'
            }
            for pattern, drink_type in additional_patterns.items():
                if pattern.lower() in description_lower:
                    found_type = drink_type.upper()
                    break
        
        return found_type



    # Function for Package Size
    def find_package_size(description):
        patterns = [
            r'\b(\d+(\.\d+)?(L|l|ltr|ltrs|liter|liters))\b',
            r'\b(\d+(\.\d+)?(ML|ml))\b',
            r'\b(\d+(\.\d+)?(OZ|oz))\b',
            r'\b(\d+(\.\d+)?(GAL|gal))\b',
            r'\b(Half\s*Barrel|Quarter\s*Barrel|Eighth\s*Barrel)\b',
            r'\b(\d+(\.\d+)?-\d+(\.\d+)?(/\w+)?)\b', 
            r'\b(\d+(\.\d+)?(\s*LTR|liter|L))\b',  
            r'\b(\d+(\.\d+)?(\s*OZ|oz))\b',  
            r'\b(\d+(\.\d+)?(\s*BTL|BTLs))\b',  
            r'\b(\d+(\.\d+)?(\s*LTR|liter|L)\s*KEG)\b', 
            r'\b(\d+(\.\d+)?\s*(LTR|liter|L)\s*(KEG))\b',  
            r'\b(\d+(\.\d+)?\s*(LTR|liter|L)\s*(BTLS|BTLs)/CS)\b',  
            r'\b(\d+(\.\d+)?(FZ|fz))\b',  
            r'\b(\d+(\.\d+)?\s*LTR/BTL)\b',  
            r'\b(\d+(\.\d+)?\s*LTR/CS)\b',  
            r'\b(\d+(\.\d+)?\s*LTRS/CS)\b',  
            r'\b(\d+(\.\d+)?\s*ML/BTL)\b',  
            r'\b(\d+(\.\d+)?\s*ML/CS)\b',  
            r'\b(\d+(\.\d+)?\s*OZ/BTL)\b',  
            r'\b(\d+(\.\d+)?\s*OZ/CS)\b',  
            r'\b(\d+(\.\d+)?\s*L/BTL)\b',  
            r'\b(\d+(\.\d+)?\s*L/CS)\b',  
            r'\b(\d+(\.\d+)?\s*ML/BTL)\b', 
            r'\b(\d+(\.\d+)?\s*ML/CS)\b',  
            r'\b(\d+(\.\d+)?\s*LTR)\b', 
            r'\b(\d+(\.\d+)?\s*ML)\b',  
            r'\b(\d+(\.\d+)?(/\d+(\.\d+)?)?\s*KEG)\b',  
            r'\b(\d+(\.\d+)?\s*CS)\b',
            r'\b(\d+(\.\d+)?\s*PKS/CS)\b', 
            r'\b(\d+(\.\d+)?\s*CS)\b',  
            r'\b(\d+(\.\d+)?\s*BTL)\b',  
            r'\b(\d+(\.\d+)?\s*LTR/CS)\b',  
            r'\b(\d+(\.\d+)?\s*BTL/CS)\b',  
            r'\b(\d+(\.\d+)?/\d+(\.\d+)?\s*(LTR|L))\b',  
            r'\b(\d+(\.\d+)?\s*BBL)\b',  
            r'\b(\d+(\.\d+)?/\d+(\.\d+)?\s*CS)\b',  
            r'\b(\d+(\.\d+)?\s*(BTL|FZ))\b',
            r'\b(\d+(\.\d+)?(/\d+(\.\d+)?))\b',
        ]
        
        
        combined_pattern = '|'.join(patterns)
        

        sizes_pattern = re.compile(combined_pattern, re.IGNORECASE)
        match = sizes_pattern.search(description)
        if match:
            return match.group().upper() 
        else:
            # Default rules as defined in the documentation
            if '750' in description:
                return '750'  
            elif 'LTR' in description:
                return 'LTR'  
            else:
                return ''  




    # Function for Size
    def find_volume_size(description):
        # Define regular expression patterns to match different size formats
        patterns = [
            r'\b(\d+(\.\d+)?(L|l|ltr|ltrs|liter|liters))\b',
            r'\b(\d+(\.\d+)?(ML|ml))\b',
            r'\b(\d+(\.\d+)?(OZ|oz))\b',
            r'\b(\d+(\.\d+)?(GAL|gal))\b',
            r'\b(Half\s*Barrel|Quarter\s*Barrel|Eighth\s*Barrel)\b',
            r'\b(\d+(\.\d+)?(/\d+(\.\d+)?))\b',  
            r'\b(\d+(\.\d+)?\s*(LTR|liter|L))\b',  
            r'\b(\d+(\.\d+)?\s*(OZ|oz))\b',  
            r'\b(\d+(\.\d+)?\s*(BTL|BTLs))\b',  
            r'\b(\d+(\.\d+)?\s*(LTR|liter|L)\s*KEG)\b',  
            r'\b(\d+(\.\d+)?\s*(LTR|L))\b',  
            r'\b(\d+(\.\d+)?\s*(ML|ml))\b',  
            r'\b(\d+(\.\d+)?\s*(GAL|gal))\b',  
            r'\b(\d+(\.\d+)?\s*(BBL))\b',  
            r'\b(\d+(\.\d+)?(FZ|fz))\b',
            r'\b(\d+(\.\d+)?(/CS|/cs))\b',
            r'\b(\d+(\.\d+)?(/BTL|/btl))\b',
            r'\b(\d+(\.\d+))\b',
        ]
        
        
        combined_pattern = '|'.join(patterns)
        
        
        sizes_pattern = re.compile(combined_pattern, re.IGNORECASE)

        
        match = sizes_pattern.search(description)
        if match:
            return match.group().upper()  
        else:
            # Default pattern as mentioned in the documentation
            if 'CS' or ' C '  in description:
                return 'CS' 
            else:
                return ''  

    # Columns Extraction

    df['Type'] = df[f"{column}"].apply(find_drink_types).astype(str)
    df['Brand'] = df[f"{column}"].apply(find_brands).astype(str)
    df['Number in case'] = df[f"{column}"].apply(extract_number)
    df['Size'] = df[f"{column}"].apply(find_volume_size).astype(str)
    df['Package Size'] = df[f"{column}"].apply(find_package_size).astype(str)


    df.to_excel('static/output.xlsx',index=False)
    print('Done Extracting..............')

    # df['Type'] = df[f"{column}"].apply(find_drink_types).astype(str)
    # df['Brand'] = df[f"{column}"].apply(find_brands).astype(str)
    # df['Number in case'] = df[f"{column}"].apply(extract_number)
    # df['Size'] = df[f"{column}"].apply(find_volume_size).astype(str)
    # df['Package Size'] = df[f"{column}"].apply(find_package_size).astype(str)


    # df.to_csv('static/output.csv',index=False)

    return jsonify('done')




@app.route("/download",methods=['GET'])
def index():
    file_path = 'static/output.xlsx'

    if os.path.exists(file_path):
        return send_file(
            file_path,
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        return jsonify("You haven't uploaded a sheet yet")
    




if __name__ == "__main__":
    app.run(debug=True)
