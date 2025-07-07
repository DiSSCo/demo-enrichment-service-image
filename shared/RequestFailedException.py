class RequestFailedException(Exception):
    """
    Exception raised when a request fails.

    Attributes:
        message (str): Explanation of the error.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"RequestFailedException: {self.message}"
