import azure.functions as func
import logging
from functions_global import read_and_convert_csv_to_json, sent_post_request_to_api

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="raw/employees/{name}.csv",
                               connection="BlobStorageConnectionString") 
def blob_trigger1(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    filename = myblob.name.split("/")[2]
    directory = myblob.name.split("/")[1]
    print(f"EL ARVHIVO ENVIADO ES:{directory}/{filename}")
    url= "http://127.0.0.1:5000/test"
    employees= read_and_convert_csv_to_json(f"{directory}",f"{filename}")
    sent_post_request_to_api(url,employees)
    
@app.blob_trigger(arg_name="myblob", path="raw/jobs/{name}.csv",
                               connection="BlobStorageConnectionString") 
def blob_trigger2(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    filename = myblob.name.split("/")[2]
    directory = myblob.name.split("/")[1]
    print(f"EL ARVHIVO ENVIADO ES:{directory}/{filename}")
    url= "http://127.0.0.1:5000/test"
    jobs= read_and_convert_csv_to_json(f"{directory}",f"{filename}")
    sent_post_request_to_api(url,jobs)

@app.blob_trigger(arg_name="myblob", path="raw/departments/{name}.csv",
                               connection="BlobStorageConnectionString") 
def blob_trigger3(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    filename = myblob.name.split("/")[2]
    directory = myblob.name.split("/")[1]
    print(f"EL ARVHIVO ENVIADO ES:{directory}/{filename}")
    url= "http://127.0.0.1:5000/test"
    departments= read_and_convert_csv_to_json(f"{directory}",f"{filename}")
    sent_post_request_to_api(url,departments)

@app.blob_trigger(arg_name="myblob", path="raw/batch/{name}.csv",
                               connection="BlobStorageConnectionString") 
def blob_trigger4(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    filename = myblob.name.split("/")[2]
    directory = myblob.name.split("/")[1]
    print(f"EL ARVHIVO ENVIADO ES:{directory}/{filename}")  
    url= "http://127.0.0.1:5000/test"
    batch= read_and_convert_csv_to_json(f"{directory}",f"{filename}")
    sent_post_request_to_api(url,batch)


    