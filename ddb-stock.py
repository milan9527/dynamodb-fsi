import boto3
import random
import time
from datetime import datetime
from decimal import Decimal

# Create a DynamoDB resource using the us-east-1 region and local AWS credentials
session = boto3.Session(region_name='us-east-1')
dynamodb = session.resource('dynamodb')

# Get the existing DynamoDB table named 'stock'
table = dynamodb.Table('stockquotes')

# List of NASDAQ stock codes
stock_codes = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'FB', 'TSLA', 'NVDA', 'INTC', 'CSCO', 'ADBE', 'NFLX', 'PYPL', 'CMCSA', 'AVGO', 'TXN', 'QCOM', 'COST', 'TMUS', 'BKNG', 'INTU', 'CHTR', 'LRCX', 'AMAT', 'ISRG', 'AMGN', 'GILD', 'VRTX', 'MDLZ', 'REGN', 'SBUX', 'BIIB', 'CELG', 'ILMN', 'MRNA', 'LULU', 'ALGN', 'MELI', 'IDXX', 'CTAS', 'SNPS', 'KLAC', 'ASML', 'MCHP', 'PAYX', 'ADSK', 'CDNS', 'XLNX', 'CTSH', 'NXPI', 'ANSS', 'VRSN', 'MRVL', 'CTXS', 'LBTYK', 'SWKS', 'VRSK', 'DLTR', 'WDAY', 'ROST', 'OKTA', 'MPWR', 'FTNT', 'TEAM', 'DXCM', 'SPLK', 'CPRT', 'GNRC', 'ZBRA', 'DOCU', 'CRWD', 'PANW', 'CDNS', 'CHKP', 'JBHT', 'SNBR', 'FFIV', 'NLOK', 'PAYC', 'NUAN', 'AKAM', 'FIVN', 'MXIM', 'MLNX', 'KLAC', 'MCHP', 'AAPL.US']

# Select 100 unique stock codes randomly
selected_stock_codes = random.sample(stock_codes, min(100, len(stock_codes)))

# Generate and insert stock data for the selected 100 stock codes every second
while True:
    current_time = int(time.time())
    stock_data_items = []

    for stock_code in selected_stock_codes:
        open_price = round(random.uniform(100, 300), 2)
        close_price = round(random.uniform(open_price - 1, open_price + 1), 2)
        high_price = round(max(open_price, close_price) + random.uniform(0, 0.5), 2)
        low_price = round(min(open_price, close_price) - random.uniform(0, 0.5), 2)
        volume = random.randint(100000, 5000000)
        turnover = volume * close_price

        stock_data_items.append({
            'code': stock_code + '.US',
            'ts': current_time,
            'open_price': Decimal(str(open_price)),
            'close_price': Decimal(str(close_price)),
            'high_price': Decimal(str(high_price)),
            'low_price': Decimal(str(low_price)),
            'volume': volume,
            'turnover': Decimal(str(turnover))
        })

    with table.batch_writer() as batch:
        for item in stock_data_items:
            batch.put_item(Item=item)

    print(f"Stock data for {len(selected_stock_codes)} codes generated at {datetime.fromtimestamp(current_time)}")
    time.sleep(1)
