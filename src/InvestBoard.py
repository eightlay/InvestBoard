import json
from typing import Optional
from tinkoffapi import TinkoffAPI
from datapreparer import concat_accounts
from errors import NoDataPathError, NoUserDataError


class InvestBoard:
    def __init__(self, data_path: str) -> None:
        self.data_path = data_path
        self.user_data = None
        self.api = None

    def start(self):
        self.read_user_data()
        self.init_api()
        data = self.read_accounts_data()
        data = concat_accounts(data)

    def read_user_data(self):
        if self.data_path:
            with open(self.data_path) as file:
                self.user_data = json.load(file)
        else:
            raise NoDataPathError(
                "Can't read data without path. Set 'data_path' first.")

    def init_api(self):
        if self.user_data:
            self.api = TinkoffAPI(self.user_data['Token'])
        else:
            raise NoUserDataError(
                "Can't init API without token. Use 'read_user_data' method first.")

    def read_accounts_data(self):
        if self.user_data:
            names = list(self.user_data['Portfolios'].keys())
            ids = list(self.user_data['Portfolios'].values())
            starting_dates = list(self.user_data['StartingDates'].values())
            return self.api.get_each_account_data(names, ids, starting_dates)
        else:
            raise NoUserDataError(
                "Can't read accounts data without portfolios' ids. \
                    Use 'read_user_data' method first.")
