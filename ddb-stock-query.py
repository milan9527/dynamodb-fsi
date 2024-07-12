import boto3
import random
import threading
import time
from datetime import datetime
from boto3.dynamodb.conditions import Key  # Import the Key object

# Create a DynamoDB resource using the us-east-1 region and local AWS credentials
session = boto3.Session(region_name='us-east-1')
dynamodb = session.resource('dynamodb')

# Get the existing DynamoDB table named 'stock'
table = dynamodb.Table('stockquotes')

# List of NASDAQ stock codes
stock_codes = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'FB', 'TSLA', 'NVDA', 'INTC', 'CSCO', 'ADBE', 'NFLX', 'PYPL', 'CMCSA', 'AVGO', 'TXN', 'QCOM', 'COST', 'TMUS', 'BKNG', 'INTU', 'CHTR', 'LRCX', 'AMAT', 'ISRG', 'AMGN', 'GILD', 'VRTX', 'MDLZ', 'REGN', 'SBUX', 'BIIB', 'CELG', 'ILMN', 'MRNA', 'LULU', 'ALGN', 'MELI', 'IDXX', 'CTAS', 'SNPS', 'KLAC', 'ASML', 'MCHP', 'PAYX', 'ADSK', 'CDNS', 'XLNX', 'CTSH', 'NXPI', 'ANSS', 'VRSN', 'MRVL', 'CTXS', 'LBTYK', 'SWKS', 'VRSK', 'DLTR', 'WDAY', 'ROST', 'OKTA', 'MPWR', 'FTNT', 'TEAM', 'DXCM', 'SPLK', 'CPRT', 'GNRC', 'ZBRA', 'DOCU', 'CRWD', 'PANW', 'CDNS', 'CHKP', 'JBHT', 'SNBR', 'FFIV', 'NLOK', 'PAYC', 'NUAN', 'AKAM', 'FIVN', 'MXIM', 'MLNX', 'KLAC', 'MCHP', 'AAPL.US']

# Convert the start and end times to Unix timestamps
start_time = int(datetime(2024, 7, 10, 13, 38, 42).timestamp())
end_time = int(datetime(2024, 7, 10, 13, 44, 42).timestamp())

# Function to send a query to DynamoDB
def send_query(partition_key, sort_key_start, sort_key_end):
    try:
        response = table.query(
            KeyConditionExpression=Key('code').eq(partition_key) & Key('ts').between(sort_key_start, sort_key_end)
        )
        print(f"Query successful for {partition_key} between {sort_key_start} and {sort_key_end}")
    except Exception as e:
        print(f"Error querying DynamoDB: {e}")

# Function to generate and send queries in a thread
def generate_and_send_queries():
    while True:
        for _ in range(10000):
            partition_key = random.choice(stock_codes) + '.US'
            sort_key_start = random.randint(start_time, end_time)
            sort_key_end = random.randint(sort_key_start, end_time)
            send_query(partition_key, sort_key_start, sort_key_end)
        time.sleep(1)

# Start multiple threads to generate and send queries
threads = []
for _ in range(10):
    thread = threading.Thread(target=generate_and_send_queries)
    threads.append(thread)
    thread.start()

# Wait for the threads to complete (which will never happen since the program runs indefinitely)
for thread in threads:
    thread.join()
