import os
# import ccxt
import rehive as rehive_sdk
import krakenex
import math
import time
from decimal import Decimal
from rehive.api.exception import NoNextException, APIException
from requests.exceptions import HTTPError
from datetime import datetime

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


REHIVE_API_KEY=os.environ.get('REHIVE_API_KEY')

# These should be input from the user interface, but for this demo they are loaded frome environmental variables:
KRAKEN_API_KEY=os.environ.get('KRAKEN_API_KEY')
KRAKEN_PRIVATE_KEY=os.environ.get('KRAKEN_PRIVATE_KEY')
NEW_USER_EMAIL=os.environ.get('NEW_USER_EMAIL')
COMPANY_ID=os.environ.get('REHIVE_API_KEY')

def main():
    # Initialize rehive sdk with admin API key
    rehive = rehive_sdk.Rehive(REHIVE_API_KEY)


    # JSON to mimick example database structure
    # This data should be pushed to a datastore like Firebase
    # This service can store multiple companies so it can be used for Baker Tilly Cayman or any other company that needs to pull client exchange data
    companies = {
        COMPANY_ID: {
            'rehive_auth_token': REHIVE_API_KEY,
            'clients': {},
            'currencies': {},
        }
    }

    new_user_email=NEW_USER_EMAIL
    kraken_api_key = KRAKEN_API_KEY
    kraken_private_key = KRAKEN_PRIVATE_KEY

    try:
        # Create client on Rehive:
        user = rehive.admin.users.create(email=new_user_email)
    except APIException:
        # Get existing client on Rehive:
        user = rehive.admin.users.get(email='mbrynard@gmail.com')[0]

    # Get currency details:
    # Cache all company currencies from Rehive:
    currencies = rehive.admin.currencies.get()
    while True:
        try:
            currencies.extend(rehive.admin.currencies.get_next())
        except NoNextException:
            break

    for currency in currencies:
        companies[COMPANY_ID]['currencies'][currency['code']] = currency

    # Add new client to "database":
    new_client = {
        user['id']: {
            'email': user['email'],
            'exchanges': {
                'kraken': {
                    'credentials':
                    {'api_key': kraken_api_key,
                     'private_key': kraken_private_key},
                    'currency_pairs': {
                        'XXBTZEUR': {
                            'code': 'XXBTZEUR',
                            'base': 'XBT',
                            'quote': 'EUR'
                        }
                    }
                }
            }
        }
    }

    companies[COMPANY_ID]['clients'].update(new_client)

    # Get client's trades:
    api_key = companies[COMPANY_ID]['clients'][user['id']]['exchanges']['kraken']['credentials']['api_key']
    private_key = companies[COMPANY_ID]['clients'][user['id']]['exchanges']['kraken']['credentials']['private_key']

    # Removed as CCXT response seems to be missing stuff:
    # kraken = ccxt.kraken({
    #     'apiKey': api_key,
    #     'secret': secret_key,
    # })
    # kraken.fetch_trades('BTC/EUR')

    kraken = krakenex.API(key=api_key, secret=private_key)
    trades = get_kraken_trade_history(kraken).values()
    for trade in trades:

        # Check if the currency pair of the trade is one in our database:
        currency_pair = companies[COMPANY_ID]['clients'][user['id']]['exchanges']['kraken']['currency_pairs'].get(trade['pair'])

        if currency_pair:
            quote_currency = currency_pair['quote']
            quote_divisibility = int(companies[COMPANY_ID]['currencies'][quote_currency]['divisibility'])
            base_currency = currency_pair['base']
            base_divisibility = int(companies[COMPANY_ID]['currencies'][base_currency]['divisibility'])

            logging.info('Uploading transaction Rehive: ' + trade['ordertxid'])

            rehive.admin.transactions.create_debit(user=user['id'],
                                                   amount=to_cents(Decimal(trade['cost']), quote_divisibility),
                                                   currency=quote_currency,
                                                   subtype='trade_debit',
                                                   metadata={'order_id': trade['ordertxid'],
                                                             'timestamp': datetime.fromtimestamp(trade['time']).isoformat()},
                                                   status='complete',
                                                   idempotent_key=trade['ordertxid']+'-'+'debit')
            rehive.admin.transactions.create_credit(user=user['id'],
                                                    amount=to_cents(Decimal(trade['vol']), base_divisibility),
                                                    currency=base_currency,
                                                    subtype='trade_credit',
                                                    metadata={'order_id': trade['ordertxid'],
                                                             'timestamp': datetime.fromtimestamp(trade['time']).isoformat()},
                                                    status='complete',
                                                    idempotent_key=trade['ordertxid']+'-'+'credit')
            rehive.admin.transactions.create_debit(user=user['id'],
                                                   amount=to_cents(Decimal(trade['fee']), quote_divisibility),
                                                   currency=quote_currency,
                                                   subtype='trade_fee',
                                                   metadata={'order_id': trade['ordertxid'],
                                                             'timestamp': datetime.fromtimestamp(trade['time']).isoformat()},
                                                   status='complete',
                                                   idempotent_key=trade['ordertxid']+'-'+'fee')

            time.sleep(2)  # Limit to 1.5 calls / second


def get_kraken_trade_history(client):
    logging.info('Fetching trade history from Kraken...')

    logging.info('Fetching batch #1')
    while True:
        try:
            response = client.query_private('TradesHistory', {})
            break
        except HTTPError as e:
            logging.warning('Kraken server error, retrying...')
            if e.response.status_code in range(500,600):
                pass
            else:
                break

    trades = response['result']['trades']

    # Each API call only gets 50 trades and so we need to do multiple
    trade_count = response['result']['count']
    iterations = int(math.ceil(float(trade_count)/50.0)) - 1

    for i in range(iterations):
        time.sleep(4)  # API call every 3 seconds to avoid rate limit
        offset = 50*(i+1)

        logging.info('Fetching batch #' + str(i+2) + '/' + str(int(trade_count/50)+1))
        while True:
            try:
                response = client.query_private('TradesHistory', {'ofs': offset})
                break
            except HTTPError as e:
                logging.warning('Kraken server error, retrying...')
                if e.response.status_code in range(500,600):
                    pass
                else:
                    break

        trades.update(response['result']['trades'])

    logging.info('Done')

    return trades


def to_cents(amount: Decimal, divisibility: int) -> int:
    return int(amount * Decimal('10')**divisibility)


def from_cents(amount: int, divisibility: int) -> Decimal:
    return Decimal(amount) / Decimal('10')**divisibility


if __name__ == '__main__':
    main()

