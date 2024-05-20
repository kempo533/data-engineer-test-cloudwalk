class APIRequestError(Exception):
    """
    Exception raised for errors during API requests.
    """

    pass


class MissingCountryIDError(Exception):
    """
    Exception raised when country ID is missing.
    """

    pass
