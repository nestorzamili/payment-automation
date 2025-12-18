from src.scrapers.axai import AxaiScraper
from src.scrapers.base import BaseScraper, get_scraper_class
from src.scrapers.fiuu import FiuuScraper
from src.scrapers.kira import KiraScraper
from src.scrapers.m1 import M1Scraper

__all__ = [
    'BaseScraper',
    'get_scraper_class',
    'KiraScraper',
    'AxaiScraper',
    'M1Scraper',
    'FiuuScraper',
]
