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


def get_boq_titles():
    url = 'https://bidplus.gem.gov.in/advance-search'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')
    table = soup.find('select', attrs={'id': 'boqtitle_con'})
    options = table.findAll('option')
    boq_titles = []
    for option in options:
        boq_title_number = option.attrs['value']
        if boq_title_number == '':
            print(f'No BOQ Title Number Found For: {option.text}')
            continue
        boq_titles.append((option.text, int(boq_title_number)))
    return boq_titles


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


def parse_formula(formula):
    negs = []
    splitted = formula.split('--')
    pos = splitted[0].split(',')
    if len(splitted) == 2:
        negs = splitted[1].split(',')
    return pos, negs


def parse_boq_titles(boq_titles, keyword_formulas):
    keyword_boq_buckets = {}
    for k in all_keyword_formulas:
        keyword_boq_buckets[k] = []
    parsed_boq_titles = []
    for (title, boq_id) in boq_titles:
        match, formula = check_keyword_formulas(title, keyword_formulas)
        if match:
            keyword_boq_buckets[formula].append(title)
            parsed_boq_titles.append((title, boq_id))
    return parsed_boq_titles, keyword_boq_buckets


def check_keyword_formulas(boq_title, keyword_formulas):
    for keyword_formula in keyword_formulas:
        pos, negs = parse_formula(keyword_formula)
        pos_hai, neg_hai = 1, 0

        # pos sare hone chahiye
        for pos_w in pos:
            if pos_w not in boq_title:
                pos_hai = 0
                break

        if not pos_hai:
            continue

        # neg ek bhi nai hona chahiye
        for neg_w in negs:
            if neg_w in boq_title:
                neg_hai = 1
                break

        if pos_hai and (not neg_hai):
            return True, keyword_formula
    return False, ''


def frequency_wordwise(paragraph):
    paragraph = paragraph.split()
    freqs = []

    for word in paragraph:
        if word not in freqs:
            freqs.append(word)

    for word in range(0, len(freqs)):
        print('Frequency of', freqs[word], 'is :', paragraph.count(freqs[word]))


def print_keyword_formula_analysis(formula, keyword_boq_buckets):
    for kk, vv in keyword_boq_buckets.items():
        print(kk, len(vv), vv)
    print(f'----------------------------{formula} ANALYSIS------------------------------')
    for title in keyword_boq_buckets[formula]:
        print(title)
    print(f'----------------------------------------------------------------------------')
    print(len(keyword_boq_buckets[formula]))
    print(frequency_wordwise(' '.join(keyword_boq_buckets[formula])))


def write_parsed_boqs(boqs):
    today = date.today()
    search_start_date = (today + datetime.timedelta(days=0)).strftime("%d-%m-%Y")
    search_end_date = (today + datetime.timedelta(days=16)).strftime("%d-%m-%Y")
    filename = f'GEM BOQS {today.strftime("%d-%m-%Y")}.csv'
    with open(filename, 'w+') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['BOQ TITLE', 'BOQ TITLE URL'])
        for boq, boq_id in boqs:
            url = f"https://bidplus.gem.gov.in/advance-search?boqtitle={boq_id}&from_date={search_start_date}&to_date={search_end_date}&searchboq=Search"
            csvwriter.writerow([boq, url])

    print(f'Saved BOQ titles to {filename}')


if __name__ == "__main__":
    all_keyword_formulas = ['FLAME', 'FIRE,FIGHTING', 'FIRE,SUIT', 'PANT', 'CLOTH', 'FABRIC--FABRICATED,FABRICATION',
                            'OVER,ALL', 'COVER,ALL', 'BOILER', 'SUIT', 'DUNGAREE', 'SLEEPING', 'SLEEPING,BAG',
                            'KIT--KITCHEN,TOOL,READY,STEP,TRAINEE,PATHOLOGY,PCR,RNA', 'RAIN--TRAINING',
                            'PONCHO', 'JACKET', 'VISIBILITY', 'VIZIBILITY', 'SAFETY--INSTALLATION', 'REFLECTIVE',
                            'LUMINOUS',
                            'GARMENTS', 'TROUSER', 'GLOVES', 'BALACLAVA',
                            'UNIFORM'
                            ]

    all_boq_titles = get_boq_titles()
    print(f'{len(all_boq_titles)} BOQ Titles found.')
    boq_titles, boq_buckets = parse_boq_titles(all_boq_titles, all_keyword_formulas)
    write_parsed_boqs(boq_titles)

# TODO: add CPPP tenders parsing at https://gem.gov.in/cppp/1?
