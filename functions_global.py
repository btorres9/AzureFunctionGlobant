from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import csv
import json
from io import StringIO
import requests

def read_and_convert_csv_to_json(directory,filename):
    # Configura la conexión al almacenamiento de blobs
    connection_string = "DefaultEndpointsProtocol=https;AccountName=saglobantchallenge;AccountKey=SXqNaIIE2KYmXWPK40l66nr+vLS636NJUX0d/Uw8ajnsSbGxSKI69jEmUyIQ39jMm0ODYRDTrahW+ASt8WjysA==;EndpointSuffix=core.windows.net"
    container_name = "raw"
    blob_name = f"{directory}/{filename}"
  
    # Crea el cliente de servicio de blob
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Accede al contenedor
    container_client = blob_service_client.get_container_client(container_name)

    # Obtiene el blob
    blob_client = container_client.get_blob_client(blob_name)

    # Descarga el contenido del blob como texto
    blob_data = blob_client.download_blob()
    csv_data = blob_data.readall().decode('utf-8')
    #print(csv_content)

    # Procesar el CSV
    csv_reader = csv.reader(csv_data.splitlines())
    # for row in csv_reader:
    #     print(row[0])
    if blob_name == "employees/hired_employees.csv" or blob_name == "batch/batch.csv":
        csv_array = []

        for row in csv_reader:
            csv_dic = {}

            csv_dic["id"] = row[0]
            csv_dic["name"]=row[1]
            csv_dic["datetime"]=row[2]
            csv_dic["department_id"]=row[3]
            csv_dic["job_id"]=row[4]

            csv_array.append(csv_dic)
            jsonfinal = json.dumps(csv_array)
    
    elif blob_name == "jobs/jobs.csv":
        csv_array = []

        for row in csv_reader:
            csv_dic = {}

            csv_dic["id"] = row[0]
            csv_dic["job"]=row[1]
            
            csv_array.append(csv_dic)
            jsonfinal = json.dumps(csv_array)
    
    elif blob_name == "departments/departments.csv":
        csv_array = []

        for row in csv_reader:
            csv_dic = {}

            csv_dic["id"] = row[0]
            csv_dic["department"]=row[1]
            
            csv_array.append(csv_dic)
            jsonfinal = json.dumps(csv_array)
    else:
        print("No llegó ningun archivo.")
    
    
    return jsonfinal



def sent_post_request_to_api(url,body):
    response = requests.post(url, json=body)


    
