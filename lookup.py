from playwright.sync_api import sync_playwright, Playwright
import time


def run(playwright: Playwright,
        url=("https://www.moex.com/ru/index/IMOEX/archive)"
             "?from=2025-01-26&till=2025-02-26&sort=TRADEDATE&order=desc")) -> str:
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


if __name__ == "__main__":
    with sync_playwright() as playwright:
        html = run(playwright)
        with open('pages/page.html', 'w', encoding='UTF-8') as f:
            f.write(html)
