from playwright.sync_api import sync_playwright, Playwright
from bs4 import BeautifulSoup, Tag
from typing import List, TypedDict, Optional, Any
from storage import Storage, StorageJSON, StorageMongo
import time
import pandas as pd
import os
import datetime


class URL:
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
    pages_path: str
    storage: Storage
    credentials: Any
    columns = [
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

    def _lookup(self, playwright: Playwright, url: URL):
        webkit = playwright.webkit
        browser = webkit.launch()
        context = browser.new_context()
        page = context.new_page()
        page.set_extra_http_headers({'User-Agent': 'Mozilla/5.0'})
        page.goto(url)

        btn_keyword = 'Согласен'
        page.get_by_text(btn_keyword, exact=True).click()
        time.sleep(3)
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
        date_column, *float_columns = df.columns
        df[date_column] = df[date_column].apply(pd.to_datetime, format='mixed')
        df[float_columns] = df[float_columns].apply(
            lambda cols: list(map(lambda x: float(x.replace(' ', '').replace(',', '.')), cols))
        )

        return df

    def _scrape_page(self, filename: str) -> Tag:
        with open(filename, 'r', encoding='UTF-8') as f:
            html = f.read()
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('div', {'class': 'ui-table__container'}).find('table')
        assert table is not None
        return table

    def load_content(self, urls: List[URL]) -> None:
        with sync_playwright() as playwright:
            for url in urls:
                html = self._lookup(playwright, url.url)
                filename = (f'{url.index_name}#from={url.date_from}&till={url.date_till}'
                            f'&sort={url.sort}&order={url.order}')
                with open(self.pages_path + filename, 'w', encoding='UTF-8') as f:
                    f.write(html)

    def scrape_pages(self, selected_pages=None) -> None:
        for filename in os.listdir(self.pages_path):
            if selected_pages is None or filename in selected_pages:
                page_table = self._scrape_page(self.pages_path + filename)
                page_df = self._convert_to_df(page_table)
                formated_df = self._parse_df(page_df)

                conn = self.storage.connect(self.credentials)
                self.storage.write(conn, formated_df, filename)
                self.storage.close(conn)
