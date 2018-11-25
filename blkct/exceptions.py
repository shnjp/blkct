# -*- coding:utf-8 -*-


class CrawlerError(Exception):
    pass


class URLAlreadyInQueue(CrawlerError):
    pass


class BadStatusCode(CrawlerError):
    pass


class ParserError(CrawlerError):
    pass


class ContextNotFoundError(CrawlerError):
    pass
