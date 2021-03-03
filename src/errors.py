# region tinkoffapy.py Errors


class ResponseError(Exception):
    """
        Raises, if response from Tinkoff OpenAPI failed.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class UnequalLengthsError(Exception):
    """
        Raises, if requested data is of different length.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


# endregion tinkoffapy.py Errors

# region datapreparer.py Errors

# endregion datapreparer.py Errors

# region TDashedInvest.py Errors


class NoDataPathError(Exception):
    """
        Raises, if no data path was provided for 'read_data' method. 
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class NoUserDataError(Exception):
    """
        Raises, if no data was provided for 'init_api' method.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


# endregion TDashedInvest.py Errors
