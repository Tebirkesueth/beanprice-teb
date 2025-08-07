"""A source fetching currency prices from FMP.

Valid tickers for currencies are in the form "XXXYYY", such as "EURUSD".

Here is the API documentation:
https://site.financialmodelingprep.com/developer/docs/stable

Timezone information: Input and output datetimes are specified via UTC
timestamps.
"""

__copyright__ = "Copyright (C) 2025 Erik Tornberg"
__license__ = "GNU GPLv2"

from datetime import datetime, timezone, timedelta
import json
import os
import requests
from decimal import Decimal, InvalidOperation
from dateutil import tz
from beanprice import source

URL = "https://financialmodelingprep.com/stable/"
API_KEY = os.getenv('FMP_API_KEY')

if API_KEY is None:
    raise ValueError("The 'FMP_API_KEY' environment variable is not set.")
    
def _fetch_latest_price(params_dict):
    """Fetch a price from FMP using the given parameters."""
    ticker = params_dict["symbol"]
    
    REQUEST_URL = rf"{URL}quote?symbol={ticker}&apikey={API_KEY}"
    response = requests.get(REQUEST_URL)
    
    if response is None and not response.status_code == 200:
        return None
    
    try:
        data = response.json()[0]
    except:
        raise FMPError(f"Error requesting {ticker} at most current date.")
    
    unixtimestamp_string = data.get('timestamp')
    price_string = data.get('price')
    
    try:
        price_date = datetime.fromtimestamp(unixtimestamp_string, tz=timezone.utc)
    except (TypeError, ValueError) as e:
        raise FMPError(f"Invalid timestamp format for {ticker}: {unixtimestamp_string}. Error: {e}")

    try:
        price_value = Decimal(str(price_string))
    except (TypeError, ValueError, InvalidOperation) as e: # InvalidOperation for Decimal
        raise FMPError(f"Invalid price format for {ticker}: {price_string}. Error: {e}")

    return source.SourcePrice(price_value, price_date, None)


def _fetch_historical_price(params_dict):
    """Fetch an historical price from FMP using the given parameters."""
    ticker = params_dict["symbol"]
    date_from = params_dict['from']
    date_to = params_dict['to']
    
    REQUEST_URL = rf"{URL}historical-price-eod/light?symbol={ticker}&from={date_from}&to={date_to}&apikey={API_KEY}"
    response = requests.get(REQUEST_URL)
    
    if response is None and not response.status_code == 200:
        return None
    
    try:
        data = response.json()[0]
    except:
        raise FMPError(f"Error requesting {ticker} between {date_from} and {date_to}.")

    date_string = data.get('date')
    price_string = data.get('price')
    
    try:
        price_date_naive = datetime.strptime(date_string, "%Y-%m-%d").date()
        price_date = datetime.combine(price_date_naive, datetime.min.time(), tzinfo=timezone.utc)
    except (TypeError, ValueError) as e:
        raise FMPError(f"Invalid timestamp format for {ticker}: {date_string}. Error: {e}")

    try:
        price_value = Decimal(str(price_string))
    except (TypeError, ValueError, InvalidOperation) as e: # InvalidOperation for Decimal
        raise FMPError(f"Invalid price format for {ticker}: {price_string}. Error: {e}")

    return source.SourcePrice(price_value, price_date, None)


def _fetch_price_series(ticker: str, time_begin: datetime, time_end: datetime):
    
    date_from = time_begin.strftime("%Y-%m-%d")
    date_to   = time_end.strftime("%Y-%m-%d")
    
    REQUEST_URL = rf"{URL}historical-price-eod/light?symbol={ticker}&from={date_from}&to={date_to}&apikey={API_KEY}"
    response = requests.get(REQUEST_URL)
    
    if response is None and not response.status_code == 200:
        return None
    
    try:
        data = response.json()
    except:
        raise FMPError(f"Error requesting {ticker} between {date_from} and {date_to}.")

    try:
        return [
            (datetime.combine(datetime.strptime(item['date'], "%Y-%m-%d").date(), datetime.min.time(), tzinfo=timezone.utc), 
             Decimal(str(item['price']))) for item in data]
    except:
        raise FMPError(f"Error iterating through {ticker} between {date_from} and {date_to} to generate tuple.")
            

class FMPError(ValueError):
    "An error from the FMP API."
    

class Source(source.Source):
    "FMP price source extractor."
        
        
    def __init__(self):
        pass
    
    
    def get_latest_price(self, ticker):
        """See contract in beanprice.source.Source."""
        time = datetime.now(tz.tzutc())
        params_dict = {
            "symbol": ticker
        }
        return _fetch_latest_price(params_dict)


    def get_historical_price(self, ticker, time):
        """See contract in beanprice.source.Source."""
        time = time.astimezone(tz.tzutc())
        query_interval_begin = time - timedelta(days=5)
        query_interval_end = time + timedelta(days=1)
        params_dict = {
            "symbol": ticker,
            "from": query_interval_begin.strftime("%Y-%m-%d"),
            "to": query_interval_end.strftime("%Y-%m-%d")
        }
        return _fetch_historical_price(params_dict)
    
    
    def get_prices_series(self, ticker, time_begin, time_end):
        """See contract in beanprice.source.Source."""
        res = [
            source.SourcePrice(x[1], x[0], None)
            for x in _fetch_price_series(ticker, time_begin, time_end)
        ]
        return sorted(res, key=lambda x: x.time)
