import enum


class CrawlPrioirty(enum.IntEnum):
    default = 1000

    high = 500
    low = 2000
    image = 5000
