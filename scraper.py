from playwright.sync_api import sync_playwright, Playwright
from bs4 import BeautifulSoup, Tag
from typing import List, TypedDict, Optional, Any
from storage import Storage, StorageJSON
import time
import pandas as pd
import os
import datetime


class URL:
    """
    Class for representing URL with query information.

    Attributes:
        BASE_URL (str): domain name of the server and path to the catalogue
        url (str): the url itself
        index_name (str): name of the index
        date_from (str): start date of the records
        date_till (str): end date of the records
        sort (str): sorting label
        order (str): sorting order ('asc' or 'desc')
    """

    BASE_URL: str = 'https://www.moex.com/ru/index/'
    url: str
    index_name: str
    date_from: str
    date_till: str
    sort: str
    order: str

    def __init__(self,
                 index_name: str,
                 date_from: str,
                 date_till: str,
                 sort: str = 'TRADEDATE',
                 order: str = 'desc'):
        self.index_name = index_name
        try:
            datetime.date.fromisoformat(date_from)
            datetime.date.fromisoformat(date_till)
        except ValueError:
            raise ValueError('Incorrect data format, should be YYYY-MM-DD')
        self.date_from = date_from
        self.date_till = date_till
        self.sort = sort
        self.order = order
        self.url = self.BASE_URL + index_name + '/archive?' + 'from=' + date_from + \
            '&till=' + date_till + '&sort=' + sort + '&order=' + order

    def construct_from_url(url: str):
        """
        Factory to construct a URL object from the url itself.
        """

        self_index_name = url[url.find("index/")+6:url.find("/archive")]
        from_info = url[url.find('from=')+5:]
        self_date_from = from_info[: from_info.find('&')]
        till_info = url[url.find('till=')+5:]
        self_date_till = till_info if till_info.find('&') == -1 else till_info[: till_info.find('&')]
        if url.find('sort=') != -1:
            sort_info = url[url.find('sort=')+5:]
            self_sort = sort_info if sort_info.find('&') == -1 else sort_info[: sort_info.find('&')]
        else:
            self_sort = 'TRADEDATE'
        if url.find('order=') != -1:
            order_info = url[url.find('order=')+6:]
            self_order = order_info
        else:
            self_order = 'desc'
        return URL(self_index_name, self_date_from, self_date_till, self_sort, self_order)


class IndexRecord(TypedDict):
    """
    Class representing a single record of an index
    """

    date: Optional[str]
    price_at_opening: Optional[str]
    max_price: Optional[str]
    min_price: Optional[str]
    price_at_closure: Optional[str]
    volume_of_trade: Optional[str]
    capitalization: Optional[str]


class Scraper:
    """
    Class to load pages, extract tables, and scrape information about indices.

    Attributes:
        pages_path (str): relative path to the directory with saved pages (*.html)
        storage (Storage): storage strategy for saving data on disk
        credentials (Any): credentials for accessing specified storage
        columns (List[str]): list of columns (see IndexRecord)
    """

    pages_path: str
    storage: Storage
    credentials: Any
    columns: List[str] = [
        'date',
        'price_at_opening',
        'max_price',
        'min_price',
        'price_at_closure',
        'volume_of_trade',
        'capitalization'
    ]

    def __init__(self,
                 pages_path: str = 'pages/',
                 storage: Storage = StorageJSON(),
                 credentials: Any = 'data/'):
        self.pages_path = pages_path
        self.storage = storage
        self.credentials = credentials

    def _lookup(self, playwright: Playwright, url: URL) -> str:
        """
        Loads a webpage, signs in, extracts content, and saves to disk.

        Attributes:
            playwright (Playwright): object to load a webpage
            url (URL): URL object of a webpage

        Returns:
            str: html content of a webpage
        """

        webkit = playwright.webkit
        browser = webkit.launch()
        context = browser.new_context()
        page = context.new_page()
        page.set_extra_http_headers({'User-Agent': 'Mozilla/5.0'})
        page.goto(url)

        btn_keyword = 'Согласен'
        page.get_by_text(btn_keyword, exact=True).click()
        time.sleep(1)
        html = page.content()

        browser.close()

        return html

    def _convert_to_df(self, soup_table: Tag) -> pd.DataFrame:
        """
        Converts a table into a pandas dataframe with string values

        Arguments:
            soup_table (Tag): table in the html format

        Returns:
            pd.Dataframe: dataframe with retrieved information
        """
        # columns = [header.text.strip() for header in soup_table.find('thead').find_all('th')]
        columns = self.columns
        assert len(columns) == 7

        data: List[IndexRecord] = []

        assert soup_table.find('tbody') is not None
        tbody = soup_table.find('tbody')
        assert tbody.find('tr') is not None

        for row in tbody.find_all('tr'):
            elements = row.find_all('td')
            assert len(elements) == 7
            data.append([element.text.strip() for element in elements])

        return pd.DataFrame(data, columns=columns)

    def _parse_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Data type conversion of the dataframe.

        Attributes:
            df (pd.DataFrame): dataframe object with the specific columns

        Returns:
            pd.DataFrame: formatted dataframe
        """

        date_column, *float_columns = df.columns
        df[date_column] = df[date_column].apply(pd.to_datetime, format='mixed')
        df[float_columns] = df[float_columns].apply(
            lambda cols: list(map(lambda x: float(x.replace(' ', '').replace(',', '.')), cols))
        )

        return df

    def _scrape_page(self, filename: str) -> Tag:
        """
        Loads a page content and scrapes table information from it.

        Attributes:
            filename (str): name of the saved page.
        
        Returns:
            Tag: table tag on the page.
        """

        with open(filename, 'r', encoding='UTF-8') as f:
            html = f.read()
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('div', {'class': 'ui-table__container'}).find('table')
        assert table is not None
        return table

    def load_content(self, urls: List[URL]) -> None:
        """
        Loads contents from the specified webpages into files on disk.

        Attributes:
            urls (List[URL]): list of URL objects representing webpages
        """

        with sync_playwright() as playwright:
            for url in urls:
                html = self._lookup(playwright, url.url)
                filename = (f'{url.index_name}#from={url.date_from}&till={url.date_till}'
                            f'&sort={url.sort}&order={url.order}')
                with open(self.pages_path + filename, 'w', encoding='UTF-8') as f:
                    f.write(html)

    def scrape_pages(self, selected_pages: List[str] = None) -> None:
        """
        Loads selected pages content from disk, scrapes information to form dataframes,
        and saves dataframes in the storage.

        Attributes:
            selected_pages (List[str]): list of names of saved pages to parse
        """

        for filename in os.listdir(self.pages_path):
            page_name = filename.split('.')[0]
            if selected_pages is None or page_name in selected_pages:
                page_table = self._scrape_page(self.pages_path + filename)
                page_df = self._convert_to_df(page_table)
                formated_df = self._parse_df(page_df)

                conn = self.storage.connect(self.credentials)
                self.storage.write(conn, formated_df, page_name)
                self.storage.close(conn)

    def load_page_data(self, page_name: str) -> pd.DataFrame:
        """
        Loads scraped page data from the storage.

        Attributes:
            page_name (str): name of the page.
        
        Returns:
            pd.DataFrame: dataframe with the information scraped from the page.
        """
        conn = self.storage.connect(self.credentials)
        df = self.storage.read(conn, page_name)
        self.storage.close(conn)
        return df
