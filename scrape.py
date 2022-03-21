import requests
from bs4 import BeautifulSoup
import csv
from datetime import date, timedelta
from datetime import datetime
import multiprocessing as mp
from os.path import exists
import traceback


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
        if 'href' not in gem_no_p.a.attrs:
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
        week_num = get_week_num(end_date)

        bids[gem_no_p.a.text] = (items.upper(), int(quantity), department, start_date, end_date, bid_doc_url, week_num)
    return bids


def get_week_num(end_date):
    try:
        week_num = datetime.strptime(end_date, '%d-%m-%Y %I:%M %p').isocalendar()[1]
    except:
        week_num = datetime.today().isocalendar()[1] + 1
    return week_num


def get_total_pages(url):
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


def extract_upcoming_bids_data1(base_url, start_page, end_page):
    print(f'DONE PAGES {start_page} to {end_page}')
    return []


def extract_upcoming_bids_data(base_url, start_page, end_page):
    all_bids = {}
    for page_num in range(start_page, end_page + 1):
        print(f'Processing page {page_num}')
        url = f"{base_url}&page_no={page_num}"
        bids = get_all_bids_from_page(url)
        print(f'Page {start_page} Bids length: {len(bids)}')
        all_bids.update(bids)
        print(f"BATCH Start {start_page}; End {end_page}; All Bids length: {len(all_bids)}")
    return all_bids


def extract_boqs_data(base_url, start_idx, end_idx, boq_titles):
    all_bids = {}
    for boq_index in range(start_idx, end_idx + 1):
        print(f'Processing BOQ index: {boq_index}; BOQ Info: {boq_titles[boq_index]}')
        url = f"{base_url}&boqtitle={boq_titles[boq_index][1]}"
        bids = get_all_bids_from_page(url)
        all_bids.update(bids)
    return all_bids


def parse_formula(formula):
    negs = []
    splitted = formula.split('--')
    pos = splitted[0].split(',')
    if len(splitted) == 2:
        negs = splitted[1].split(',')
    return pos, negs


def parse_boq_titles(boq_titles, keyword_formulas):
    keyword_boq_buckets = {}
    for k in ALL_KEYWORD_FORMULAS:
        keyword_boq_buckets[k] = []
    parsed_boq_titles = []
    for (title, boq_id) in boq_titles:
        match, formula = check_keyword_formulas(title, keyword_formulas)
        if match:
            keyword_boq_buckets[formula].append(title)
            parsed_boq_titles.append((title, boq_id))
    return parsed_boq_titles, keyword_boq_buckets


def parse_bids(bids, keyword_formulas):
    keyword_bid_buckets = {}
    for k in ALL_KEYWORD_FORMULAS:
        keyword_bid_buckets[k] = []
    parsed_bids = {}
    for bid_num in bids:
        match, formula = check_keyword_formulas(bids[bid_num][0], keyword_formulas)
        if match:
            keyword_bid_buckets[formula].append(f'{bids[bid_num][0]};{bid_num}')
            parsed_bids[bid_num] = bids[bid_num]
    return parsed_bids, keyword_bid_buckets


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
    search_start_date = (today + timedelta(days=0)).strftime("%d-%m-%Y")
    search_end_date = (today + timedelta(days=16)).strftime("%d-%m-%Y")
    filename = f'GEM BOQS {today.strftime("%d-%m-%Y")}.csv'
    with open(filename, 'w+') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['BOQ TITLE', 'BOQ TITLE URL'])
        for boq, boq_id in boqs:
            url = f"https://bidplus.gem.gov.in/advance-search?boqtitle={boq_id}&from_date={search_start_date}&to_date={search_end_date}&searchboq=Search"
            csvwriter.writerow([boq, url])

    print(f'Saved BOQ titles to {filename}')


def get_tenders_for_boqs(boq_titles):
    boqs_per_batch = 5
    num_batches = 7
    pool = mp.Pool(num_batches + 1)

    start, end = get_offset_dates(0, 100)
    url = f'https://bidplus.gem.gov.in/advance-search?from_date={start}&to_date={end}&searchboq=Search'

    total_pages = len(boq_titles)

    for b_batch_page_offset in range(0, total_pages, boqs_per_batch * num_batches):
        b_batch_num = int(b_batch_page_offset / boqs_per_batch / num_batches)
        print(
            f'-----------------------TIME STATS START BATCH NUM {b_batch_num} {datetime.now().strftime("%d.%b %Y %H:%M:%S")}')
        if PARALLEL_MODE:
            results = search_all_boqs_parallel(pool, url, boqs_per_batch, total_pages, b_batch_page_offset,
                                               num_batches, boq_titles)
        else:
            results = search_all_boqs_seq(url, boqs_per_batch, total_pages, b_batch_page_offset, num_batches,
                                          boq_titles)
        print(
            f'-----------------------TIME STATS END BATCH NUM {b_batch_num} {datetime.now().strftime("%d.%b %Y %H:%M:%S")}')
        write_results_to_files(results)
        print(f'Results len {len(results)} ---- {",".join(str(len(ra)) for ra in results)}')


def run_boq_search():
    all_boq_titles = get_boq_titles()
    print(f'{len(all_boq_titles)} BOQ Titles found.')
    boq_titles, boq_buckets = parse_boq_titles(all_boq_titles, ALL_KEYWORD_FORMULAS)
    print(f'BOQ Titles filtered: {len(boq_titles)}')
    get_tenders_for_boqs(boq_titles)


def get_offset_dates(s_offset, e_offset):
    today = date.today()
    start = (today + timedelta(days=s_offset)).strftime("%d-%m-%Y")
    end = (today + timedelta(days=e_offset)).strftime("%d-%m-%Y")
    return start, end


def merge_results(results):
    bids = {}
    for r in results:
        for b_num in r:
            bids[b_num] = r[b_num]
    return bids


def get_week_nums(bids):
    week_nums = set()
    for bid_num in bids:
        week_nums.add(bids[bid_num][6])
    return week_nums


def write_bid_to_file(bid_num, bid_info, writer, today):
    writer.writerow([bid_num, bid_info[0], bid_info[1], bid_info[2], bid_info[3], bid_info[4], bid_info[5], today])


def write_results_to_files(results):
    bids = merge_results(results)
    today = date.today().strftime("%d-%m-%Y")
    parsed_bids, keyword_bid_buckets = parse_bids(bids, ALL_KEYWORD_FORMULAS)
    if len(parsed_bids) == 0:
        return

    all_csv_writers = get_all_file_writers(parsed_bids)

    for bid_num in parsed_bids:
        bid_info = parsed_bids[bid_num]
        existing_file_bids = all_csv_writers[bid_info[6]][1]
        if bid_num in existing_file_bids:
            continue
        write_bid_to_file(bid_num, bid_info, all_csv_writers[bid_info[6]][0], today)


def get_bids_from_file(opened_file):
    reader = csv.reader(opened_file)
    bids = set()
    for row in reader:
        bids.add(row[0])
    return bids


def get_all_file_writers(parsed_bids):
    week_nums = get_week_nums(parsed_bids)
    files = {}
    for week in week_nums:
        filename = f'ALL BIDS OPEN WEEK {week}.csv'
        if exists(filename):
            existing_bids_in_file = get_bids_from_file(open(filename, 'r'))
        else:
            existing_bids_in_file = set()

        files[week] = (csv.writer(open(filename, 'a')), existing_bids_in_file)

    return files


def search_all_bids_parallel(pool, url, pages_per_batch, total_pages, b_batch_page_offset, num_batches):
    while True:
        try:
            results = pool.starmap_async(extract_upcoming_bids_data,
                                         [(url, i, min(i + pages_per_batch - 1, total_pages))
                                          for i in range(1 + b_batch_page_offset,
                                                         min(pages_per_batch * num_batches + b_batch_page_offset,
                                                             total_pages + 1),
                                                         pages_per_batch)]).get()
        except:
            tb = traceback.format_exc()
            print(f'!!!!!!!! Exception Thrown !!!!!!!!!')
            print(tb)
            print(f'!!!!!!!! Retrying !!!!!!!!!')
            continue
        return results


def search_all_boqs_parallel(pool, url, pages_per_batch, total_pages, b_batch_page_offset, num_batches, boq_titles):
    while True:
        try:
            results = pool.starmap_async(extract_boqs_data,
                                         [(url, i, min(i + pages_per_batch - 1, total_pages - 1), boq_titles)
                                          for i in range(b_batch_page_offset,
                                                         min(pages_per_batch * num_batches + b_batch_page_offset - 1,
                                                             total_pages),
                                                         pages_per_batch)]).get()
        except:
            tb = traceback.format_exc()
            print(f'!!!!!!!! Exception Thrown !!!!!!!!!')
            print(tb)
            print(f'!!!!!!!! Retrying !!!!!!!!!')
            continue
        return results


def search_all_bids_seq(url, pages_per_batch, total_pages, b_batch_page_offset, num_batches):
    while True:
        try:
            results = [extract_upcoming_bids_data(url, i, min(i + pages_per_batch - 1, total_pages))
                       for i in range(1 + b_batch_page_offset,
                                      min(pages_per_batch * num_batches + b_batch_page_offset,
                                          total_pages + 1),
                                      pages_per_batch)]
        except:
            tb = traceback.format_exc()
            print(f'!!!!!!!! Exception Thrown !!!!!!!!!')
            print(tb)
            print(f'!!!!!!!! Retrying !!!!!!!!!')
            continue
        return results


def search_all_boqs_seq(url, pages_per_batch, total_pages, b_batch_page_offset, num_batches, boq_titles):
    while True:
        try:
            results = [extract_boqs_data(url, i, min(i + pages_per_batch - 1, total_pages - 1), boq_titles)
                       for i in range(b_batch_page_offset,
                                      min(pages_per_batch * num_batches + b_batch_page_offset - 1,
                                          total_pages),
                                      pages_per_batch)]
        except:
            tb = traceback.format_exc()
            print(f'!!!!!!!! Exception Thrown !!!!!!!!!')
            print(tb)
            print(f'!!!!!!!! Retrying !!!!!!!!!')
            continue
        return results


def run_weekwise_all_bids_search():
    pages_per_batch = 5
    num_batches = 7
    pool = mp.Pool(num_batches + 1)

    start, end = get_offset_dates(7, 14)
    url = f'https://bidplus.gem.gov.in/advance-search?from_date={start}&to_date={end}&searchbid=Search'
    print(url)

    total_pages = get_total_pages(url)

    print(f'Total pages: {total_pages}')

    for b_batch_page_offset in range(0, total_pages, pages_per_batch * num_batches):
        b_batch_num = int(b_batch_page_offset / pages_per_batch / num_batches)
        print(
            f'-----------------------TIME STATS START BATCH NUM {b_batch_num} {datetime.now().strftime("%d.%b %Y %H:%M:%S")}')
        if PARALLEL_MODE:
            results = search_all_bids_parallel(pool, url, pages_per_batch, total_pages, b_batch_page_offset,
                                               num_batches)
        else:
            results = search_all_bids_seq(url, pages_per_batch, total_pages, b_batch_page_offset, num_batches)
        print(
            f'-----------------------TIME STATS END BATCH NUM {b_batch_num} {datetime.now().strftime("%d.%b %Y %H:%M:%S")}')
        write_results_to_files(results)
        print(f'Results len {len(results)} ---- {",".join(str(len(ra)) for ra in results)}')


if __name__ == "__main__":
    ALL_KEYWORD_FORMULAS = ['FLAME', 'FIRE,FIGHTING', 'FIRE,SUIT', 'PANT', 'CLOTH', 'FABRIC--FABRICATED,FABRICATION',
                            'OVER,ALL', 'COVER,ALL', 'BOILER', 'SUIT', 'DUNGAREE', 'SLEEPING', 'SLEEPING,BAG',
                            'KIT--KITCHEN,TOOL,READY,STEP,TRAINEE,PATHOLOGY,PCR,RNA', 'RAIN--TRAINING',
                            'PONCHO', 'JACKET', 'VISIBILITY', 'VIZIBILITY', 'SAFETY--INSTALLATION', 'REFLECTIVE',
                            'LUMINOUS',
                            'GARMENTS', 'TROUSER', 'GLOVES', 'BALACLAVA',
                            'UNIFORM'
                            ]
    PARALLEL_MODE = 1

    run_weekwise_all_bids_search()
    run_boq_search()

# TODO: add CPPP tenders parsing at https://gem.gov.in/cppp/1?
# TODO: read the full item name, right now - its not reading the item names from the correct place!
