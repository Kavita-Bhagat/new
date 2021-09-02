import requests  # allows user to send HTTP requests using Python
import pandas as pd # used to analyze data.
import pickle
from bs4 import BeautifulSoup
import boto3
from dynamo_funcs import get_dynamo_result, decimal_to_float
from boto3.dynamodb.conditions import Key, Attr, And

client = boto3.client('lambda')
s3_client = boto3.client('s3')

TABLE_NAME = "parsed_cv_data"


def getlocation(): 
    #Location
    wikiurl="https://en.wikipedia.org/wiki/List_of_cities_in_India_by_population"
    response=requests.get(wikiurl)
    soup = BeautifulSoup(response.text, 'html.parser')
    indiatable=soup.find('table',{'class':"wikitable"} )

    df=pd.read_html(str(indiatable))
    # convert list to dataframe
    df=pd.DataFrame(df[0])
    loc= df["City"].values.tolist()
    return loc

    # filter_exp = {}
    # filter_exp["FilterExpression"] = Attr("fullParsed.experiences.location").eq(str(location))
    # for n in loc:
    #     if n == location:
    #         print(n) 
        

    # response = get_dynamo_result(
    # TABLE_NAME,
    # type_="scan",
    # Select='SPECIFIC_ATTRIBUTES',
    # ReturnConsumedCapacity='SPECIFIC_ATTRIBUTES',
    # ProjectionExpression= "type_n_sha2_512, fullParsed.experiences.location"
    # )
    
def _year_of_exp_startDate(type_n_sha2_512):
    response = get_dynamo_result(
    TABLE_NAME,
    type_="scan",
    ReturnConsumedCapacity='TOTAL',
    Select='SPECIFIC_ATTRIBUTES',
    KeyConditionExpression= Key('id').eq(str(type_n_sha2_512)),
    ProjectionExpression= "fullParsed.experiences.startDate,fullParsed.experiences.endDate" #, fullParsed.experiences.endDate"
    )
    if not response: return None
    elif len(response) == 1:
        res = response[0]
        return decimal_to_float(res)

def _year_of_exp_endDate(type_n_sha2_512):
    response = get_dynamo_result(
    TABLE_NAME,
    type_="scan",
    ReturnConsumedCapacity='TOTAL',
    Select='SPECIFIC_ATTRIBUTES',
    KeyConditionExpression= Key('id').eq(str(type_n_sha2_512)),
    ProjectionExpression= "fullParsed.experiences.endDate"
    )
    if not response: return None
    elif len(response) == 1:
        res = response[0]
        return decimal_to_float(res)
 
def _year_calculation(startDate,endDate):
    pass

def year_of_exp(type_n_sha2_512):

    startDate = _year_of_exp_startDate(type_n_sha2_512)
    endDate = _year_of_exp_endDate(type_n_sha2_512)


def tier_of_college(self):
    #Tier of college
    bucket = ''
    key = ''
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body']
    data = pickle.loads(content.read())
 
def nos_of_jobs_or_intern(type_n_sha2_512,type):
    #  Number of job/internships, - from parser - two varibales - int
    response = get_dynamo_result(
    TABLE_NAME,
    type_="scan",
    ReturnConsumedCapacity='TOTAL',
    Select='SPECIFIC_ATTRIBUTES',
    KeyConditionExpression= Key('id').eq(str(type_n_sha2_512)),
    ProjectionExpression= "fullParsed.full_experience.company,fullParsed.experiences.company,"
    )
    if not response:
        return None
    elif len(response) == 1:
        if type == "job":
            response = len(list(response)[0]["fullParsed.full_experience.company"])
            return response
        else:
            response = len(list(response)[0]["fullParsed.experiences.company"])
            return response

def prev_job_roles(type_n_sha2_512):
    
    # res = get_dynamo_result(
    #     TABLE_NAME,
    #     type_="scan",
    #     Select='SPECIFIC_ATTRIBUTES',
    #     # Limit=1,
    #     # ConsistentRead=False,
    #     # ScanIndexForward=False,
    #     ReturnConsumedCapacity='TOTAL',
    #     ProjectionExpression="fullParsed,type_n_sha2_512"
        
    # )
    # res = pd.DataFrame(res)
    # res["type_n_sha2_512"] = res["type_n_sha2_512"].map(str)
    # return res.set_index("type_n_sha2_512")["fullParsed"].tolist()
    # res = get_dynamo_result(
    #     TABLE_NAME,
    #     type_="scan",
    #     Select='SPECIFIC_ATTRIBUTES',
    #     # Limit=1,
    #     # ConsistentRead=False,
    #     # ScanIndexForward=False,
    #     ReturnConsumedCapacity='TOTAL',
    #     ProjectionExpression="fullParsed,type_n_sha2_512"
        
    # )
    # res = pd.DataFrame(res)
    # res["type_n_sha2_512"] = res["type_n_sha2_512"].map(str)
    # return res.set_index("type_n_sha2_512")["fullParsed"].to_dict()
    
    
    response = get_dynamo_result(
        TABLE_NAME,
        type_="query",
        Select='SPECIFIC_ATTRIBUTES',
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="fullParsed",
        KeyConditionExpression=Key('type_n_sha2_512').eq(str(type_n_sha2_512)),
    )
    if not response:
        return None
    elif len(response) == 1:
        response = list(response)[0]["fullParsed"]["experiences"]
        for n in response:
            # #k= 
            # if (len(n["title"]) == 1):
            #     res = n["title"][0]
            #     print(res)
            #     return res
            # # if k:
            # #     return n["title"]
            # #continue   
            return  n["title"]     
            
        
#=-----------------------
    # res = get_dynamo_result(
    #     TABLE_NAME,
    #     type_="scan",
    #     Select='SPECIFIC_ATTRIBUTES',
    #     # Limit=1,
    #     # ConsistentRead=False,
    #     # ScanIndexForward=False,
    #     ReturnConsumedCapacity='TOTAL',
    #   # KeyConditionExpression= Key('id').eq(str(type_n_sha2_512)),
    #     ProjectionExpression="fullParsed,type_n_sha2_512"
    # )
    # res = pd.DataFrame(res)
    # res["type_n_sha2_512"] = res["type_n_sha2_512"].map(str)
    # return res.set_index("type_n_sha2_512")["type_n_sha2_512"].tolist()

    
# Prev job roles/designations
def get_pathway_insights(type_n_sha2_512):
    """[summary]

    Args:
        pathwayid ([type]): [description]
        
    """
    response = get_dynamo_result(
        TABLE_NAME,
        type_="query",
        Select='SPECIFIC_ATTRIBUTES',
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="basicParsed",
        KeyConditionExpression=Key('type_n_sha2_512').eq(str(type_n_sha2_512)),
    )
    if not response: return None
    elif len(response) == 1:
        res = response[0]
        return decimal_to_float(res)
