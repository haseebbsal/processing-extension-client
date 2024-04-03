from flask import Flask,jsonify,request,send_file
from flask_cors import CORS
import pandas as pd
import re
from worker import conn
from rq import Queue
from rq.job import Job
import boto3
from io import BytesIO
import os

 

app = Flask(__name__)


CORS(app)

aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY")
bucket=os.getenv("S3_BUCKET")

queue = Queue(connection=conn)


def uploadBrand():
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access,
                      aws_secret_access_key=aws_secret)
    new_df=list(pd.read_csv('https://markjbs.s3.us-west-2.amazonaws.com/brands.csv',encoding='iso-8859-1',header=None)[0])
    brand_lower_set = set(new_df)
    brand_new_lower_set = set(pd.read_csv('https://markjbs.s3.us-west-2.amazonaws.com/brands1.csv',encoding='iso-8859-1',header=None)[0])
    brand_lower_set=brand_lower_set.union(brand_new_lower_set)
    brand_dataframe=pd.DataFrame(list(brand_lower_set))
    csv_buffer=BytesIO()
    brand_dataframe.to_csv(csv_buffer,index=False)
    csv_buffer.seek(0)
    s3.delete_object(
        Bucket='markjbs',
        Key='brands1.csv'
    )
    s3.put_object(Bucket="markjbs",
                        Key="brands.csv",
                        Body=csv_buffer)
    return 'done'

def uploadDrink():
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access,
                      aws_secret_access_key=aws_secret)
    new_df=list(pd.read_csv('https://markjbs.s3.us-west-2.amazonaws.com/drink_types.csv',encoding='iso-8859-1',header=None)[0])
    brand_lower_set = set(new_df)
    brand_new_lower_set = set(pd.read_csv('https://markjbs.s3.us-west-2.amazonaws.com/drink_types1.csv',encoding='iso-8859-1',header=None)[0])
    brand_lower_set=brand_lower_set.union(brand_new_lower_set)
    brand_dataframe=pd.DataFrame(list(brand_lower_set))
    csv_buffer=BytesIO()
    brand_dataframe.to_csv(csv_buffer,index=False)
    csv_buffer.seek(0)
    s3.delete_object(
        Bucket='markjbs',
        Key='drink_types1.csv'
    )
    s3.put_object(Bucket="markjbs",
                        Key="drink_types.csv",
                        Body=csv_buffer)
    return 'done'

def testingg(column):
    
    drink_types_lower = set(pd.read_csv('https://markjbs.s3.us-west-2.amazonaws.com/drink_types.csv',encoding='iso-8859-1',header=None)[0])
    

    csv_file_path = 'https://markjbs.s3.us-west-2.amazonaws.com/brands.csv'
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
            
    
    df=pd.read_csv('https://markjbs.s3.us-west-2.amazonaws.com/input.csv',encoding='iso-8859-1',dtype='object')
                
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
        def low(string):
            return str(string).lower()
        brand_supplier=pd.read_csv('https://markjbs.s3.us-west-2.amazonaws.com/Brand-supplier.csv',dtype='object')
        manufacturer_list=brand_supplier['Manufacturer']
        manufacturer_to_insert=[]
        brands_to_search=list(brand_supplier['Brand'].apply(low))
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

@app.route('/fuck', methods=['GET'])
def checkagainnnn():
    return jsonify('workingggg')


@app.route('/uploadBrand',methods=['GET'])
def uploadingBrand():
    job = queue.enqueue(uploadBrand,job_timeout='5h')
    id=job.get_id()
    print('job id',id)
    return jsonify({
        'jobId':id,
        'status':200
    })

@app.route('/uploadDrink',methods=['GET'])
def uploadingDrink():
    job = queue.enqueue(uploadDrink,job_timeout='5h')
    id=job.get_id()
    print('job id',id)
    return jsonify({
        'jobId':id,
        'status':200
    })
    

@app.route('/process', methods=['POST'])
def create_and_save_excel():

    column = request.form['column']
    job = queue.enqueue(testingg,column,job_timeout='5h')
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
            s3 = boto3.client('s3',
                      aws_access_key_id=aws_access,
                      aws_secret_access_key=aws_secret)
            output_dataframe=pd.DataFrame(actual_data,columns=job.result['columns'])
            csv_buffer=BytesIO()
            output_dataframe.to_csv(csv_buffer,index=False)
            csv_buffer.seek(0)
            s3.put_object(Bucket="markjbs",
                        Key="output.xlsx",
                        Body=csv_buffer)
            # output_dataframe.to_excel('static/output.xlsx',index=False)



    print(job.get_id())
    
    return jsonify(response_object)

@app.route('/getResult-files', methods=['POST'])
def get_result():
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


