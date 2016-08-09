import boto3
import json
import decimal

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('movie')
#with table.batch_writer() as batch:
with open("moviedata.json") as json_file:
        jq = json.load(json_file, parse_float=decimal.Decimal)

# print jq
for i in jq:
    year = i['year']
    title = i['title']
    info = i['info']
    
    table.put_item(
             Item={
             'year': year,
             'title': title,
             'info': info,
        # 'image_url' : image_url,
        # 'plot' : plot,
        # 'rank' : rank,
        # 'running_time_secs' : running_time_secs
    }
    )
