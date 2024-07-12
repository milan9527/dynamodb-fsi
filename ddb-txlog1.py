import boto3
from datetime import datetime, timezone
import uuid
from decimal import Decimal, ROUND_HALF_UP

# Create a DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Replace with your desired region

# Get the existing DynamoDB table
table_name = 'txlog'
table = dynamodb.Table(table_name)

# Function to store a transaction log in DynamoDB
def store_transaction_log(in_hash, out_hash, total_amount, currency):
    transaction_id = str(uuid.uuid4())  # Generate a unique transaction ID
    ts = int(datetime.now(timezone.utc).timestamp())  # Convert current time to Unix timestamp

    total_out = Decimal(str(total_amount)).quantize(Decimal('.00000001'), rounding=ROUND_HALF_UP)  # Round to 8 decimal places
    total_in = Decimal('0').quantize(Decimal('.00000001'), rounding=ROUND_HALF_UP)  # Initialize total_in as Decimal

    # Calculate the exchange fee (0.015% of the transaction amount)
    exchange_fee = total_out * Decimal('0.00015')
    exchange_fee = exchange_fee.quantize(Decimal('.00000001'), rounding=ROUND_HALF_UP)

    # Create "out" item
    item_tx_id = str(uuid.uuid4())  # Generate a unique item transaction ID
    item_ts = int(datetime.now(timezone.utc).timestamp())  # Generate a unique timestamp for the item

    out_item = {
        'tx': item_tx_id,
        'ts': item_ts,
        'in_hash': in_hash,
        'out_hash': out_hash,
        'amount': total_out,
        'currency': currency,
        'spent': exchange_fee
    }

    # Put the "out" item in the DynamoDB table
    table.put_item(Item=out_item)
    print(f'Transaction log stored: {out_item}')

    # Create "in" item
    item_tx_id = str(uuid.uuid4())  # Generate a unique item transaction ID
    item_ts = int(datetime.now(timezone.utc).timestamp())  # Generate a unique timestamp for the item

    in_item = {
        'tx': item_tx_id,
        'ts': item_ts,
        'in_hash': in_hash,
        'out_hash': out_hash,
        'amount': total_out,
        'currency': currency,
        'spent': exchange_fee
    }

    # Put the "in" item in the DynamoDB table
    table.put_item(Item=in_item)
    print(f'Transaction log stored: {in_item}')

    total_in += total_out

    # Set a small tolerance or epsilon value
    epsilon = Decimal('0.00000001')

    # Check if the difference between total_in and total_out is within the tolerance
    assert abs(total_in - total_out) <= epsilon, f"Total incoming amount ({total_in}) doesn't match total outgoing amount ({total_out})"

# Example transactions
# Transaction 1
in_hash = '0x0123456789abcdef'
out_hash = '0xfedcba9876543210'
store_transaction_log(in_hash, out_hash, 1.0, 'BTC')

# Transaction 2
in_hash = '0xfedcba9876543210'
out_hash = '0x0987654321abcdef'
store_transaction_log(in_hash, out_hash, 2.5, 'ETH')

# Transaction 3
in_hash = '0x0987654321abcdef'
out_hash = '0xfedcba0987654321'
store_transaction_log(in_hash, out_hash, 3.75, 'BTC')
