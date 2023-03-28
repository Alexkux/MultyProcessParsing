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
        'authority': 'tachka.ru',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'ru,en;q=0.9',
        # 'cookie': '_ym_uid=1669358802662288893; _ym_d=1669358802; supportOnlineTalkID=iAHZHLG5D2SuB5bcyZ4ia1UMonBvEekk; _jsuid=4208243538; mmg=0; language=ru; _gid=GA1.2.1232516251.1672027706; _ym_isad=2; PHPSESSID=010a8fab558550093ca6c5a256234294; _ga_JNNER3BHS4=GS1.1.1672055466.7.0.1672055466.60.0.0; _ga=GA1.2.1543746765.1669358802; _gat=1; _heatmaps_g2g_100846186=no',
        'referer': 'https://tachka.ru/',
        'sec-ch-ua': '"Chromium";v="106", "Yandex";v="22", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 YaBrowser/22.11.3.818 Yowser/2.5 Safari/537.36',
    }

    r = requests.get(url, headers=headers)  # Response
    sleep(uniform(4, 9))

    return r.text  # возвращает html код страницы


def get_all_links(url_file):  # тут подтягиваем ссылки для парсинга в массив links
    with open(url_file) as file:
        links = [row.strip() for row in file]

    print(links)
    print(f'Количество ссылок для парсинга: {len(links)}')
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
                'tech': '',
                'link': ''
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
                tech = tech.replace('\n\n\n', '; ')
                tech = tech.replace('\n', ' - ')
            except:
                tech = ''

            try:
                link = product.find('a').get('href')
                # for link in soup.find_all('a'):
                # link = product.get('href')
                # link = product.find('a', {"class": "link"})
            except:
                link = 'n'

            try:
                data = {'category': category,
                        'sku': sku,
                        'name': name,
                        'price': price,
                        'quantity': quantity,
                        'manufacturer': manufacturer,
                        'tech': tech,
                        'link': link
                        }
            except Exception:
                continue

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
                'tech': '',
                'link': ''
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
                         data['tech'],
                         data['link']))
        print(data['name'], data['link'], 'parsed')


def make_all(url):
    print(url)
    html = get_html(url)
    data = get_page_data(html)


def main():
    start = datetime.now()

    url_file = 'links_for_scan.txt'
    all_links = get_all_links(url_file)

    with Pool(6) as p:  # Pool(10) - количество выполняемых процессов
        p.map(make_all, all_links)

    end = datetime.now()
    total = end - start
    print(f"\nВыполнено за {total} секунд")


if __name__ == '__main__':
    main()
