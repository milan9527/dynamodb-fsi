import boto3
from datetime import datetime, timedelta
from decimal import Decimal

# Create a DynamoDB resource using the us-east-1 region and local AWS credentials
session = boto3.Session(region_name='us-east-1')
dynamodb = session.resource('dynamodb')

# Get the existing DynamoDB table named 'stockquotes'
table = dynamodb.Table('stockquotes')

# Function to get the K-chart data for a stock in one-minute intervals
def get_k_chart_data(stock_code, start_time, end_time):
    k_chart_data = []
    current_time = start_time

    while current_time < end_time:
        next_time = current_time + 60  # Next minute

        # Query the DynamoDB table for the stock data within the one-minute interval
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('code').eq(stock_code + '.US') & boto3.dynamodb.conditions.Key('ts').between(current_time, next_time - 1),
            ProjectionExpression='open_price, close_price, high_price, low_price, volume, turnover',
            ScanIndexForward=True
        )

        if response['Items']:
            open_price = response['Items'][0]['open_price']
            close_price = response['Items'][-1]['close_price']
            high_price = max(item['high_price'] for item in response['Items'])
            low_price = min(item['low_price'] for item in response['Items'])
            volume = sum(item['volume'] for item in response['Items'])
            turnover = sum(item['turnover'] for item in response['Items'])

            k_chart_data.append({
                'timestamp': current_time,
                'open': float(open_price),
                'close': float(close_price),
                'high': float(high_price),
                'low': float(low_price),
                'volume': volume,
                'turnover': float(turnover)
            })

        current_time = next_time

    return k_chart_data

# Example usage
stock_code = 'AMZN'
#start_time = int(datetime(2024, 7, 12, 15, 19).timestamp())  # May 1, 2023, 9:30 AM
#end_time = int(datetime(2024, 7, 12, 15, 20).timestamp())  # May 1, 2023, 10:30 AM
start_time = 1720797732
end_time = 1720797792


k_chart_data = get_k_chart_data(stock_code, start_time, end_time)

for data_point in k_chart_data:
    print(data_point)
