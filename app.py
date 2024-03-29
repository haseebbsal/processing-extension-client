from flask import Flask, render_template,jsonify,request,send_file
from flask_cors import CORS
import pandas as pd
import numpy as np
import csv
import re
from worker import conn
from rq import Queue
from rq.job import Job
import os
import boto3



# Configuration for the upload folder
 

app = Flask(__name__)

MYDIR = os.path.dirname(__file__)

app.config['UPLOAD_FOLDER'] = 'static'

CORS(app)
import os

queue = Queue(connection=conn)

def testingg(column):
    
    drink_types_lower = set(pd.read_csv('static/drink_types.csv',encoding='iso-8859-1',header=None)[0])
    

    csv_file_path = 'static/brands.csv'
    brand_name_list=list(pd.read_csv(csv_file_path,encoding='iso-8859-1',header=None)[0])

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



    # # Function For Brand
    def find_brands(description):
        brands_list = ''
        for brand_name in brand_name_list:
            print(str(brand_name))
            print(description.lower())
            if str(brand_name).lower() in description.lower():
                brands_list+=(str(brand_name).upper())
                break 
       
        
        return brands_list


    # # Function for Drink Type
    def find_drink_types(description):
        found_type = None
        
        description_lower = description.lower()

        for drink in drink_types_lower:
            if str(drink).lower() in description_lower:
                found_type = str(drink).upper()
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


    # #     # Columns Extraction
            
    
    df=pd.read_csv('static/input.csv')
                
    try:
        type_array= df[f"{column}"]
        new_type_array= []
        new_brand_type=[]
        new_number_type=[]
        new_size_type=[]
        new_package_size=[]
        for i in range(len(type_array)):
            
            new_type_array.append(str(find_drink_types(str(type_array[i]))))
            new_brand_type.append(str(find_brands(str(type_array[i]))))
            new_number_type.append(str(extract_number(str(type_array[i]))))
            new_size_type.append(str(find_volume_size(str(type_array[i]))))
            new_package_size.append(str(find_package_size(str(type_array[i]))))
            
        
        df['Type'] = new_type_array
        df['Brand'] = new_brand_type
        df['Number in case'] = new_number_type
        df['Size'] = new_size_type
        df['Package Size'] = new_package_size
            
        # column_data,brand_name_list,drink_types_lower,manufacturer_list,columns
        def low(string):
            return str(string).lower()
        brand_supplier=pd.read_csv('static/Brand-supplier master list.csv',dtype='object')
        manufacturer_list=brand_supplier['Manufacturer']
        manufacturer_to_insert=[]
        brands_to_search=list(brand_supplier['Brand'].apply(low))
        # brands_to_search=list(map(low,brand_list))
        storage_of_brands_finished=[]
        storage_of_manufacture_of_brands=[]
        for i in new_brand_type:
            longest_manufacturer=''
            brand=str(i).lower()
            imp_count=storage_of_brands_finished.count(brand)
            if not (imp_count>0):
                count=brands_to_search.count(brand)
                if count>0:
                    for j in range(count):
                        position_of_brand=brands_to_search.index(brand)
                        manufacturer=str(manufacturer_list[position_of_brand])
                        print(manufacturer)
                        if len(manufacturer)>len(longest_manufacturer):
                            if manufacturer!='nan':
                                longest_manufacturer=manufacturer
                        brands_to_search[position_of_brand]=''
                manufacturer_to_insert.append(longest_manufacturer)
                if brand not in storage_of_brands_finished:
                    storage_of_brands_finished.append(brand)
                    storage_of_manufacture_of_brands.append(longest_manufacturer)
            else:
                imp_index=storage_of_brands_finished.index(brand)
                manufacturer_to_insert.append(storage_of_manufacture_of_brands[imp_index])
        df['Manufacturer']=manufacturer_to_insert


        # df.to_excel('static/output.xlsx', index=False)
        print('done')

        return {
            "data":df.values.tolist(),
            "columns":df.columns.tolist(),
            "status":200

        }
    except Exception as e:
        print(e)
        # print('error is here')
        return {
            "status":400
        }





@app.route('/get', methods=['GET'])
def check():
    return jsonify('working')

@app.route('/gettt', methods=['GET'])
def checkagain():
    return jsonify('workingggg')

@app.route('/uploadFiles',methods=['POST'])
def savingFiles():
    obj= boto3.client("s3")
    drink_types_csv = 'static/drink_types.csv'

    drink_types_lower = set(pd.read_csv(drink_types_csv,encoding='iso-8859-1',header=None)[0])
    

    csv_file_path = 'static/brands.csv'
    brand_name_list=list(pd.read_csv(csv_file_path,encoding='iso-8859-1',header=None)[0])
    try:
        brand_file=request.files['brand-file']
        brand_file_path = 'static/random.csv'
        brand_file.save(os.path.join(MYDIR, app.config['UPLOAD_FOLDER'], 'random.csv'))
        # brand_file.save(brand_file_path)
        brand_type_csv = 'static/brands.csv'
        brand_lower_set = set(brand_name_list)
        brand_new_lower_set = set(pd.read_csv(brand_file_path,encoding='iso-8859-1',header=None)[0])
        brand_lower_set=brand_lower_set.union(brand_new_lower_set)
        brand_dataframe=pd.DataFrame(list(brand_lower_set))
        brand_dataframe.to_csv(brand_type_csv,index=False)
        os.remove(brand_file_path)
    except:
       pass
    try:
        drink_file=request.files['drink-file']
        drink_file_path = 'static/random1.csv'
        drink_file.save(os.path.join(MYDIR, app.config['UPLOAD_FOLDER'], 'random1.csv'))
        # drink_file.save(drink_file_path)
        
        drink_types_new_lower = set(pd.read_csv(drink_file_path,encoding='iso-8859-1',header=None)[0])
        
        drink_types_lower=drink_types_lower.union(drink_types_new_lower)
        drink_dataframe=pd.DataFrame(list(drink_types_lower))
        drink_dataframe.to_csv(drink_types_csv,index=False)
        os.remove(drink_file_path)
    except:
       pass

    try:
        file = request.files['file']
        file_contents=file.read()
        s3 = boto3.client('s3',
                      aws_access_key_id='AKIA4BVLINANA5WQAY7U',
                      aws_secret_access_key='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')
        s3.put_object(Bucket="markjbs",
                      Key="inputt.csv",
                      Body=file_contents)
        # file.save('static/input.csv')
        # obj.upload_file(
        #     Filename="input.csv",
        #     Bucket="markjbs",
        #     Body=file_contents
        # )
    except:
        pass

    return jsonify({
        "status":200
    })


@app.route('/process', methods=['POST'])
def create_and_save_excel():

    column = request.form['column']
    job = queue.enqueue(testingg,column,job_timeout='1h')
    id=job.get_id()
    print('job id',id)
    return jsonify({
        'jobId':id,
        'status':200
    })


@app.route('/get-result', methods=['POST'])
def getting_result():
    id=request.json
    print(id)
    job = Job.fetch(id['job_key'], connection=conn)

    response_object = {
        "status": "success",
        "data": {
            "job_id": job.get_id(),
            "job_status": job.get_status(),
            "job_result": job.result,
        },
    }

    if(job.result):

        if job.result['status']==200:
            data=job.result['data']
            actual_data=[]
            for i in data:
                filter=[]
                for j in i:
                    
                    filter.append(str(j))
                actual_data.append(filter)

            response_object = {
                "status": "success",
                "data": {
                    "job_id": job.get_id(),
                    "job_status": job.get_status(),
                    "job_result": actual_data,
                },
            }
            output_dataframe=pd.DataFrame(actual_data,columns=job.result['columns'])
            output_dataframe.to_excel('static/output.xlsx',index=False)



    print(job.get_id())
    
    return jsonify(response_object)



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


