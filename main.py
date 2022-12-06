import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
from multiprocessing import Pool



def get_html(url):
    r = requests.get(url)  # Response
    return r.text  # возвращает html код страницы


def get_all_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    tds = soup.find('tbody').find_all('tr', class_='cmc-table-row')
    links = []

    for td in tds:
        a = td.find('a').get('href')
        link = 'https://coinmarketcap.com' + a
        links.append(link)
    return links


def get_page_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    try:
        name = soup.find('span', class_='sc-1d5226ca-1').text.strip()
    except:
        name = ''
    try:
        price = soup.find('div', class_='priceValue').text.strip()
    except:
        price = ''

    data = {'name': name,
            'price': price}
    return data

def write_csv(data):
    with open('coinmarketcap.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow( (data['name'],
                          data['price']) )
        print(data['name'], 'parsed')


def make_all(url):
    html = get_html(url)
    data = get_page_data(html)
    write_csv(data)



def main():
    start = datetime.now()
    url = 'https://coinmarketcap.com/all/views/all/'
    all_links = get_all_links(get_html(url))

    # for index, url in enumerate(all_links):
    #     html = get_html(url)
    #     data = get_page_data(html)
    #     write_csv(data)
    #     print(index)

    with Pool(10) as p:
        p.map(make_all, all_links)
        


    end = datetime.now()
    total = end - start
    print(str(total))

if __name__ == '__main__':
    main()
