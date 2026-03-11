class FileIntakeError(Exception):
    """Base class for all file intake errors. Catch this to handle any intake failure."""


class EmptyFileError(FileIntakeError):
    """File exists but contains zero bytes."""


class CorruptFileError(FileIntakeError):
    """
    File exists and is non-empty but fails a basic integrity check.
    Raised by format engines when a file cannot be parsed despite passing intake.
    Examples: binary content behind a .csv extension, truncated upload mid-row.
    """


class UnsupportedEncodingError(FileIntakeError):
    """
    File encoding cannot be determined and the UTF-8 fallback also failed.
    Raised only when the sniff sample cannot be decoded by any attempted codec.
    """
