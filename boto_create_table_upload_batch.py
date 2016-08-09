import boto3
import json
import decimal


# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName='complete',
    KeySchema=[
        {
            'AttributeName': 'year',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'title',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'year',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='complete')

# Print out some data about the table.
print(table.item_count)
table = dynamodb.Table('complete')
with table.batch_writer() as batch:
        with open("moviedata.json") as json_file:
            jq = json.load(json_file, parse_float=decimal.Decimal)

                # print jq
        for i in jq:
            year = i['year']
            title = i['title']
            info = i['info']

            batch.put_item(
              Item={
              'year': year,
              'title': title,
              'info': info
            }
         )
