import numpy as np
import pandas as pd
from decimal import Decimal
from pycbrf.toolbox import ExchangeRates
from typing import List, Dict, Optional, Any, Tuple
from tinvest.schemas import PortfolioPosition, Operation


def concat_accounts(toconcat_:
                    Dict[str, List[Any]]) -> List[pd.DataFrame]:
    """
        Concat portfolios, operations, exchange rates tables of
        several accounts ().
        Does not change any original data of DataPreparer object.

        Parameters
        ----------
        - toconcat_ : Dict[str, List[Any]]
                      portfolios, operations, exchange rates tables
                      for each account

        Returns
        -------
        concated portfolios, operations, exchange rates tables
        {List[pd.DataFrame]}
    """
    # Prepare each account data
    toconcat = []
    for key, val in toconcat_.items():
        toconcat.append(_prepare_data(*val, account_name=key))

    # Concat data
    toconcat = np.array(toconcat, dtype='object')
    result = [None, None, None]
    for col in range(len(result)):
        result[col] = pd.concat(toconcat[:, col], ignore_index=True)

    return result


def _prepare_data(portfolio: List[PortfolioPosition], 
                  operations: List[Operation],
                  exchange_rates: Dict[str, Decimal], 
                  account_name: Optional[str] = None) -> None:
    """
        Transform all required data to pd.DataFrame and ensures currency
        comparability, if each data tables were provided.

        Parameters
        ----------
        - account_name : str, optional
                            account name to add it to tables
    """
    data = _to_DataFrame(data=(portfolio, operations, exchange_rates),
                         account_name=account_name)
    return list(_currency_comparability(data))


def _to_DataFrame(data: Tuple[List], account_name: Optional[str] = None) -> None:
    """
        Transform all data tables to pd.DataFrame.
        Add account name to tables, of provided.

        Parameters
        ----------
        - account_name : str, optional
                            account name to add it to tables
    """
    portfolio, operations, exchange_rates = data

    # Portfolio to dataframe
    rows = []
    for p in portfolio:
        typ = str(p.instrument_type)
        typ = typ[typ.find('.') + 1:]
        currency = p.expected_yield.currency
        currency = currency[currency.find('.') + 1:]
        rows.append([p.figi,
                     p.ticker,
                     p.name,
                     p.average_position_price.value,
                     p.expected_yield.value,
                     p.average_position_price.value * p.balance + p.expected_yield.value,
                     currency,
                     p.balance,
                     typ])
    portfolio = pd.DataFrame(rows,
                                  columns=['figi', 'ticker',
                                           'name', 'cost',
                                           'yield', 'value',
                                           'currency', 'balance',
                                           'instrument'])
    if account_name:
        portfolio['account'] = account_name

    # Operations to dataframe
    rows = []
    for o in operations:
        typ = str(o.operation_type)
        typ = typ[typ.find('.') + 1:]
        currency = o.currency
        currency = currency[currency.find('.') + 1:]
        inst_type = str(o.instrument_type)
        inst_type = inst_type[inst_type.find(
            '.') + 1:] if inst_type else None
        rows.append([o.id,
                     o.figi,
                     o.date,
                     o.payment,
                     o.commission.value if o.commission else None,
                     currency,
                     o.quantity_executed,
                     inst_type,
                     typ])
    operations = pd.DataFrame(rows,
                                   columns=['id', 'figi', 'date',
                                            'payment', 'commission',
                                            'currency', 'quantity',
                                            'instrument', 'operation'])
    if account_name:
        operations['account'] = account_name

    # Exchange rates to dataframe
    exchange_rates = pd.DataFrame(exchange_rates.items(),
                                       columns=['currency', 'exchange_rate'])
    if account_name:
        exchange_rates['account'] = account_name

    return (portfolio, operations, exchange_rates)


def _currency_comparability(data: Tuple[List]) -> None:
    """
        Ensure currency comparability of data tables.
    """
    portfolio, operations, exchange_rates = data

    # Portfolio
    portfolio = pd.merge(
        left=portfolio,
        right=exchange_rates.drop(
            columns='account'
        ),
        how='left',
        left_on='currency',
        right_on='currency'
    ).fillna(1.0)

    portfolio['value_RUB'] = portfolio.value * \
        portfolio.exchange_rate

    RUB_positions = portfolio.currency == 'RUB'

    for row in exchange_rates.values:
        name = 'value_{}'.format(row[0])
        portfolio[name] = portfolio.value_RUB / row[1]
        portfolio.loc[RUB_positions,
                           name] = portfolio.loc[RUB_positions, 'value_RUB']

    portfolio = portfolio.drop(columns='exchange_rate')

    # Operations
    operations['exchange_rate'] = operations[['date', 'currency']].apply(
        lambda x: ExchangeRates(x[0])[x[1]].rate if x[1] != 'RUB' else 1,
        axis=1
    ).astype('float64')

    operations['payment_RUB'] = operations.payment * \
        operations.exchange_rate
    operations['commission_RUB'] = operations.commission * \
        operations.exchange_rate

    RUB_positions = operations.currency == 'RUB'

    for currency in exchange_rates.currency:
        operations.exchange_rate = operations[['date', 'currency']].apply(
            lambda x: ExchangeRates(
                x[0])[currency].rate if x[1] != 'RUB' else 1,
            axis=1
        ).astype('float64')
        name_currency = 'payment_{}'.format(currency)
        name_commission = 'commission_{}'.format(currency)
        operations[name_currency] = operations.payment_RUB / \
            operations.exchange_rate
        operations[name_commission] = operations.commission_RUB / \
            operations.exchange_rate

    operations = operations.drop(columns='exchange_rate')

    return (portfolio, operations, exchange_rates)
