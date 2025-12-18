class ScraperError(Exception):
    pass


class LoginError(ScraperError):
    pass


class DownloadError(ScraperError):
    pass


class SessionError(ScraperError):
    pass


class ConfigurationError(Exception):
    pass


class ProcessingError(Exception):
    pass


class MergeError(ProcessingError):
    pass


class SheetsError(Exception):
    pass


class UploadError(SheetsError):
    pass
