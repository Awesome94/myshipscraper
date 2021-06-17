import requests
import pandas as pd
from bs4 import BeautifulSoup


def get_all_pages(url, pages_arr=[]):
    '''
    A recursive function that check the pagination tab
    for the next page. 
    Returns an array of all available urls for accessing
    the next pages.
    '''
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    urls = soup.find(class_="paging_column_center center")
    new_url = ''

    pages_arr.append(url)
    
    #check next page link in the pagination section untill it nolonger redirects
    for x in urls.find_all("a"):
        if x.text == '»' and x['class'] == ['no_redirect']:
            return pages_arr
        if x.text == '»' and x['class'] != ['no_redirect']:
            new_url = "https://www.myshiptracking.com"+x['href']
    return get_all_pages(new_url, pages_arr)


def clean_data_from_soup(soup, data):
    '''
    Function cleans/formats data that has been
    scraped from the website using bs4 and 
    returns an array containing rows(arrays) of
    all the data that has been scraped.
    '''
    table_data = soup.find('table', class_="table_main")

    for table_row in table_data.find_all('tr'):
        row_data = table_row.find_all('td')
        row = [tr.text.strip() for tr in row_data]
        if len(row):
            vessel_name = table_row.find_all(
                class_='table_title table_vessel_title')[0].text
            row[1] = vessel_name
            data.append(row)
    return data


def parse_to_csv(url):
    '''
    Function uses Pandas library to write
    the scraped data to a .csv file.
    '''
    soup = ''
    data = []
    headers = []
    expected_headers = ['Name', 'MMSI', 'IMO',
                        'Size', 'Loc / Dest', 'Received']

    all_pages_urls = get_all_pages(url) #get current page link and all the next pages

    for page_url in all_pages_urls:
        page = requests.get(page_url)

        if page.status_code != requests.codes.ok:
            print('FETCHING DATA FAILED, Please try again')
            return

        soup = BeautifulSoup(page.text, 'html.parser')
        formated_data = clean_data_from_soup(soup, data)

    print('FORMATING DATA COMPLETED')

    # set headers for the pandas dataframe
    if soup:
        table = soup.find('table', class_="table_main")
        for theader in table.find_all('th'):
            header = theader.text.strip()
            headers.append(header)

    df = pd.DataFrame(columns=headers)

    for data in formated_data:
        length = len(df)
        df.loc[length] = data
    df = df.loc[:, ~df.columns.duplicated()].filter(items=expected_headers)

    print('PANDAS DATA FRAME')
    print(df)

    return df.to_csv(r'vessel_data.csv')


if __name__ == '__main__':
    url = "https://www.myshiptracking.com/vessels?name=&destination=mombasa&sf=0&st=60&sort_dir=DESC&sort=RECEIVED&fbuild=1880&tbuild=2021"
    parse_to_csv(url)
