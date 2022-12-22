import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
from multiprocessing import Pool
import re
from time import sleep
from random import uniform


def get_html(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'If-Modified-Since': 'Tue, 20 Dec 2022 03:36:18 GMT',
        'If-None-Match': 'W/63a12db2-3879',
    }

    r = requests.get(url, headers=headers)  # Response
    sleep(uniform(3, 6))

    return r.text  # возвращает html код страницы


def get_all_links(url_file):  # тут подтягиваем ссылки для парсинга в массив links
    with open(url_file) as file:
        links = [row.strip() for row in file]

    print(links)
    print(f'Количество ссылок для парсинга: {len(links)}')
    # print(type(links))

    # soup = BeautifulSoup(html, 'html.parser')
    # tds = soup.find('tbody').find_all('tr', class_='cmc-table-row')
    # links = []
    #
    # for td in tds:
    #     a = td.find('a').get('href')
    #     link = 'https://coinmarketcap.com' + a
    #     links.append(link)
    return links


def get_page_data(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        data = {'category': '',
                'sku': '',
                'name': '',
                'price': '',
                'quantity': '',
                'manufacturer': '',
                'tech': ''
                }
        return data

    products = soup.find_all("div", {"class": "catalog-item"})  # Получаем массив экземпляров карточки товара

    # page += 1
    # print(len(products), 'стр:', 'ссылка:', url)
    if len(products) > 0:

        for product in products:  # Открываем каждую карточку для парсинга
            try:
                category = soup.find_all("span", {"itemprop": "name"})
                category = str(category)
                category = re.sub("[^А-я, " "]", "", category)
            except:
                category = ''

            try:
                name = product.find('h3', class_='catalog-item__head').text.strip()
            except:
                name = ''

            try:
                k = name.rfind('Артикул ')  # переменная для вычисления позиции подстроки
                sku = name[k + 8:]  # значение артикула вырезаем из наименования
            except:
                sku = ''

            try:
                price = product.find("div", {"class": "catalog-item__price"}).text.strip().replace("₽",
                                                                                                   "")  # получаем значение цены и обрезаем лишние символы
            except:
                price = ''

            try:
                quantity = product.find("span",
                                        {"class": "catalog-item__quantity-count"})  # получаем значение остатка продукта
                # quantity = soup.find("span", class_='catalog-item__quantity-count').text.strip()
                quantity = str(quantity)  # переопределяем тип значения остатка
                quantity = re.sub("[^0-9]", "", quantity)  # удаление всех символов из строки, кроме цыфр
            except:
                quantity = 0  # если в количество ничего не записано, присваеваем ему значение 0

            try:
                specific = product.find("div", {"class": "catalog-item__attributes attributes"}).text.strip()
                specific = str(specific)
                specific = re.sub('\n|\s', '', specific)
                m = specific.rfind('ль:')
                manufacturer = specific[m + 3:]
            except:
                manufacturer = ''

            try:
                tech = product.find("div", {"class": "tech"}).text.strip()
                # tech = re.sub('\n|\s', '', tech)
                tech = tech.replace('\n\n\n', '; ')
                tech = tech.replace('\n', ' - ')
            except:
                tech = ''

            try:
                data = {'category': category,
                        'sku': sku,
                        'name': name,
                        'price': price,
                        'quantity': quantity,
                        'manufacturer': manufacturer,
                        'tech': tech
                        }
            except Exception:
                continue
                # data = {'category': '',
                #         'sku': '',
                #         'name': '',
                #         'price': '',
                #         'quantity': '',
                #         'manufacturer': ''}
            # parsed =

            try:
                write_csv(data)
            except Exception:
                continue
    else:
        data = {'category': '',
                'sku': '',
                'name': '',
                'price': '',
                'quantity': '',
                'manufacturer': '',
                'tech': ''
                }

    # print(data)
    return data


def write_csv(data):
    with open('out_file.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow((data['category'],
                         data['sku'],
                         data['name'],
                         data['price'],
                         data['quantity'],
                         data['manufacturer'],
                         data['tech']))
        print(data['name'], 'parsed')


def make_all(url):
    print(url)
    html = get_html(url)
    data = get_page_data(html)


#  write_csv(data)


def main():
    start = datetime.now()

    url_file = 'links_for_scan.txt'
    all_links = get_all_links(url_file)
    # print(all_links)
    # print(type(all_links))
    # for index, url in enumerate(all_links):
    #     html = get_html(url)
    #     data = get_page_data(html)
    #     write_csv(data)
    #     print(index)

    with Pool(1) as p:  # Pool(10) - количество выполняемых процессов
        p.map(make_all, all_links)

    end = datetime.now()
    total = end - start
    print(f"\nВыполнено за {total} секунд")


if __name__ == '__main__':
    main()
