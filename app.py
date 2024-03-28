from flask import Flask, render_template,jsonify,request,send_file
from flask_cors import CORS
import pandas as pd
import numpy as np
import csv
import re
from worker import conn
from rq import Queue
from rq.job import Job
 

app = Flask(__name__)

CORS(app)
import os

queue = Queue(connection=conn)


def processing(columnn,filee,brand_filee,drink_filee):
    return 'wowwwww'
    


@app.route('/get', methods=['GET'])
def check():
    return jsonify('working')

@app.route('/gettt', methods=['GET'])
def checkagain():
    return jsonify('workingggg')

@app.route('/upload', methods=['POST'])
def create_and_save_excel():
    print('im here')
    column = request.form['column']
    file = request.files['file']
    brand_file=None
    drink_file=None
    try:
        brand_file=request.files['brand-file']
    except:
       pass
    try:
        drink_file=request.files['drink-file']
    except:
       pass
    job = queue.enqueue(
			processing,column,file,brand_file,drink_file)
    return jsonify({
        'jobId':job.get_id(),
        'status':200
    })


@app.route('/get-result', methods=['POST'])
def getting_result():
    id=request.json
    print(id)
    job = Job.fetch(id['job_key'], connection=conn)
	
	# # If job exists then return job id and status along with result
	# # But result will only be present if job has actually finished
	# # So this logic checking will be done by the poller function at client-side
    response_object = {
        "status": "success",
        "data": {
            "job_id": job.get_id(),
            "job_status": job.get_status(),
            "job_result": job.result,
        },
    }

    print(job.get_id())
    # else:
    #     response_object = {"status": "error"}
    # response_object = {"status": id['job_key']}
    
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

