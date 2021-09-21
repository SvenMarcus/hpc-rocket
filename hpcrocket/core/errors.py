def error_type(err: Exception) -> str:
    """
    Returns the type of the exception as a string.
    """
    return type(err).__name__


def get_error_message(err: Exception) -> str:
    """
    Returns the error message of the exception as a string in the form '{error_type(err)}: {str(err)}'
    """
    type_name = type(err).__name__
    error_string = f"{type_name}: {err}"
    return error_string
