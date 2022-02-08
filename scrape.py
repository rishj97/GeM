import requests
from bs4 import BeautifulSoup
import csv
from datetime import date
import datetime


def get_all_bids_from_page(url):
    bids = {}
    r = requests.get(url)

    soup = BeautifulSoup(r.content, 'lxml')

    table = soup.find('div', attrs={'id': 'pagi_content'})
    if table is None:
        return bids
    for row in table.findAll('div', attrs={'class': 'border block'}):
        gem_no_header = row.find('div', attrs={'class': 'block_header'})
        gem_no_p = gem_no_header.find('p', attrs={'class': 'bid_no pull-left'})
        if 'href' not in gem_no_p.a:
            bid_doc_url = 'n/a'
        else:
            bid_doc_url = 'https://bidplus.gem.gov.in' + gem_no_p.a['href']
        col_blocks = row.findAll('div', attrs={'class': 'col-block'})
        item_n_quantity = col_blocks[0].text
        department_n_address = col_blocks[1].text
        start_n_end_date = col_blocks[2].text

        items_start = item_n_quantity.find('Items:') + len('Items:')
        items_end = item_n_quantity.find('Quantity Required:')
        quantity_start = items_end + len('Quantity Required:')
        items = item_n_quantity[items_start: items_end].strip()
        quantity = item_n_quantity[quantity_start:].strip()

        dept_start = department_n_address.find('Department Name And Address:') + len('Department Name And Address:')
        department = department_n_address[dept_start:].strip()

        start_date_start = start_n_end_date.find('Start Date:') + len('Start Date:')
        start_date_end = start_n_end_date.find('End Date:')
        end_date_start = start_date_end + len('End Date:')
        start_date = start_n_end_date[start_date_start: start_date_end].strip()
        end_date = start_n_end_date[end_date_start:].strip()

        bids[gem_no_p.a.text] = (items, int(quantity), department, start_date, end_date, bid_doc_url)
    return bids


def get_total_pages():
    url = 'https://bidplus.gem.gov.in/bidlists?bidlists'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')
    table = soup.find('ul', attrs={'class': 'pagination'})
    lis = table.findAll('li')
    return int(lis[-1].a['data-ci-pagination-page'])


def extract_upcoming_bids_data(start_page, end_page):
    today = date.today()
    search_start_date = (today + datetime.timedelta(days=1)).strftime("%d-%m-%Y")
    search_end_date = (today + datetime.timedelta(days=16)).strftime("%d-%m-%Y")

    all_bids = {}

    with open(f'GEM ALL BIDS {search_start_date} TO {search_end_date}.csv', 'w+') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['GEM ID', 'ITEM', 'QUANTITY', 'DEPARTMENT', 'START DATE', 'END DATE', 'BID DOC URL'])
        for page_num in range(start_page, end_page + 1):
            print(f'Processing page {page_num}')
            url = f"https://bidplus.gem.gov.in/advance-search?bno=&category=&from_date={search_start_date}&to_date={search_end_date}&searchbid=Search&page_no={page_num}"
            bids = get_all_bids_from_page(url)
            print(f'Bids length: {len(bids)}')
            for gem_no in bids:
                bid_info = bids[gem_no]
                csvwriter.writerow(
                    [gem_no, bid_info[0], bid_info[1], bid_info[2], bid_info[3], bid_info[4], bid_info[5]])
            all_bids.update(bids)
            print(f'All Bids length: {len(all_bids)}')


if __name__ == "__main__":
    # total_pages = get_total_pages()
    extract_upcoming_bids_data(1, 300)


# TODO: add CPPP tenders parsing at https://gem.gov.in/cppp/1?

