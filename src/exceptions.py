class APIRequestError(Exception):
    """
    Exception raised for errors during API requests.

    Attributes:
        message (str): Explanation of the error.
    """

    pass


class MissingCountryIDError(Exception):
    """
    Exception raised when country ID is missing.

    Attributes:
        message (str): Explanation of the error.
    """

    pass
