from botocore.exceptions import ClientError
import boto3
import json 
import csv 
import re

#{'docname': '1256664a-5a1b-45a9-91e2-6b223615dd5d', 'id': '744bd0e0333406ab2f3736a21660067d', 'url': 
# 'https://disqus.com/by/disqus_mj2e1uGmlN/', 'startIdx': Decimal('42224270'), 'contentHash': '9159469', 'endIdx': Decimal('42227955')}

fields = ['docname', 'id', 'url', 'startIdx', 'contentHash', 'endIdx']
fields2 = ['id', 'contents']

def write_csv_file(filename, data):
    with open(filename, 'w') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(fields)
        for row in data:
            csvwriter.writerow([
                row["docname"], 
                row["id"], 
                row["url"], 
                row["startIdx"], 
                row["contentHash"], 
                row["endIdx"]]
            )

docnames = set()
def get_unique_docnames(data):
    for row in data:
        docnames.add(row['docname'])

    write_docnames()

def write_docnames():
    with open('uniqueDocnames.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['docname'])
        for n in docnames:
            csvwriter.writerow([n])
    
    #process_s3_files()

def convert_to_single_line(lines):
    return ''.join(lines.splitlines())

s3_client = boto3.client('s3',aws_access_key_id='AKIAZOHLA4EYU4BHGTX3', aws_secret_access_key='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', region_name='us-east-1')
def process_s3_files(filename, readFromFile=False):
    if (readFromFile):
        with open('uniqueDocnames.csv') as f:
            reader = csv.reader(f)
            for row in reader:
                docnames.add(''.join(row))
    
    with open(filename, 'w') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(['id', 'contents'])
        for docname in docnames:
            try:
                obj = s3_client.get_object(Bucket="555finalproject", Key="documents-final/" + docname + ".txt")
                j = obj['Body'].read().decode('utf-8')       
                idxs = ([m.start() for m in re.finditer(re.escape('*startid'), j)])
                for i in range(0, len(idxs) - 1):
                    curr_section = j[idxs[i]:idxs[i+1]]
                    docid = curr_section.partition('\n')[0].split("*startid")[1].split("*")[0]
                    contents = convert_to_single_line(curr_section.partition('\n')[2])
                    csvwriter.writerow([docid, contents])
            except ClientError as ex:
                continue


def get_data_from_dynamodb():
    dynamodb = boto3.resource('dynamodb',aws_access_key_id='AKIAZOHLA4EYU4BHGTX3', aws_secret_access_key='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', region_name='us-east-1')
    doc_table = dynamodb.Table('documents-final')
    response = doc_table.scan()
    data = response['Items']
    # paginate response
    while response.get('LastEvaluatedKey'):
        response = doc_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    get_unique_docnames(data)

    #write_csv_file("test.csv", data)

if __name__ == "__main__":
    #get_data_from_dynamodb()
    process_s3_files("crawlInput.csv", True)
