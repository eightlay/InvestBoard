import tinvest
from http import HTTPStatus
from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Any, Optional
from errors import ResponseError, UnequalLengthsError


def parse_response(response: tinvest.sync_client.ResponseWrapper) -> Any:
    """
        Parse response from Tinkoff OpenAPI.
        Raise ResponseError if request failed.

        Parameters
        ----------
        - repsonse : tinvest.sync_client.ResponseWrapper
                     response to parse

        Returns
        -------
        any {parsed data payload}
    """
    if response.status_code == HTTPStatus.OK:
        return response.parse_json().payload
    raise ResponseError(response.parse_error().json())


class TinkoffAPI:
    def __init__(self, api_token: str) -> None:
        """
            Inititate TinkoffAPI object. Create synchronic client with
            given token (get it from Tinkoff Invest)

            Parameters
            ----------
            - api_token : str
                          token from Tinkoff Invest
        """
        self._client = tinvest.SyncClient(api_token)
        self._broker_account_id = None
        self._starting_date = None
        self._ending_date = None
        self._basic_currencies_figi = set(['BBG0013HGFT4',   # USD
                                           'BBG0013HJJ31'])  # EUR

    def get_each_account_data(self,
                              names: List[str],
                              ids: List[int],
                              starting_dates: List[str])\
        -> Dict[str, List[Any]]:
        """
            Get portfolio, operations, exchange rates for each requested
            broker account.
            Does not change any original settings of TinkoffAPI object.

            Parameters
            ----------
            - name : List[str]
                     names of accounts
            - ids : List[int]
                    ids of accounts
            - starting_dates : List[int]
                               starting dates of accounts

            Returns
            -------
            portfolio, operations, exchange rates for each account
            {Dict[str, List[Any]}
        """
        if len(names) == len(ids) == len(starting_dates):
            # Remember original settings
            rem_id = self._broker_account_id
            rem_sd = self._starting_date
            rem_ed = self._ending_date

            # Get new data
            result = {}
            for name, id, date in zip(names, ids, starting_dates):
                self.switch_account(id, date)
                result[name] = self.request_data()

            # Restore original settings
            self._broker_account_id = rem_id
            self._starting_date = rem_sd
            self._ending_date = rem_ed

            # Return new data
            return result

        raise UnequalLengthsError(
            "Names, ids and starting dates lists must have equal length.")

    def switch_account(self,
                       new_broker_account_id: int,
                       new_starting_date: str,
                       new_ending_date: Optional[str] = None) -> None:
        """
            Switch broker account with id, account starting date
            and (optional) ending date.

            Parameters
            ----------
            - new_broker_account_id : str
                                       broker account id to switch
            - new_starting_date : str
                                  account starting date
            - new_ending_date : str, optional
                                last date to read operaions
                                today if None passed
        """
        self._broker_account_id = new_broker_account_id
        self._starting_date = datetime.strptime(new_starting_date, "%d.%m.%Y")
        if new_ending_date:
            self._ending_date = datetime.strptime(new_ending_date, "%d.%m.%Y")
        else:
            self._ending_date = datetime.now()

    def request_data(self) -> List[Any]:
        """
            Request portfolio, operations, exchange rates from Tinkoff Invest.

            Returns
            -------
            portfolio, operations, exchange rates
            {List[Any]}
        """
        portfolio = self.portfolio_get()
        operations = self.operations_get()
        exchange_rates = self.exchange_rates_get()
        return [portfolio, operations, exchange_rates]

    def portfolio_get(self) -> List[tinvest.schemas.PortfolioPosition]:
        """
            Request portfolio from Tinkoff Invest.

            Returns
            -------
            portfolio List[tinvest.schemas.PortfolioPosition]
        """
        api = tinvest.PortfolioApi(self._client)
        response = api.portfolio_get(self._broker_account_id)
        portfolio = parse_response(response)
        portfolio = portfolio.positions
        return portfolio

    def operations_get(self) -> List[tinvest.schemas.Operation]:
        """
            Request operations from Tinkoff Invest.

            Returns
            -------
            operations List[tinvest.schemas.Operation]
        """
        api = tinvest.OperationsApi(self._client)
        response = api.operations_get(
            broker_account_id=self._broker_account_id,
            from_=self._starting_date,
            to=self._ending_date)
        operations = parse_response(response)
        operations = operations.operations
        return operations

    def exchange_rates_get(self) -> Dict[str, Decimal]:
        """
            Request exchange rates from Tinkoff Invest.

            Returns
            -------
            exchange rates Dict[str, Decimal]
        """
        api = tinvest.MarketApi(self._client)
        exchange_rates = {}
        for figi in self._basic_currencies_figi:
            # Get ticker
            response = api.market_search_by_figi_get(figi)
            ticker = parse_response(response)
            ticker = ticker.ticker[:3]

            # Get exchange rate
            response = api.market_orderbook_get(figi, depth=1)
            exchange_rate = parse_response(response)
            exchange_rate = exchange_rate.last_price

            # Store exchange rate by ticker
            exchange_rates[ticker] = exchange_rate

        return exchange_rates
