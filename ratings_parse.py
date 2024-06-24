# Начало: импорты, функции, глобальные переменные

import pandas as pd
import requests
import re

from tqdm import tqdm
from time import sleep, strftime
from random import randint
from bs4 import BeautifulSoup
from yandex_reviews_parser.utils import YandexParser

import warnings

warnings.filterwarnings('ignore')

headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0"}
session = requests.Session()  # Создаем сессию
session.headers = headers  # Передать заголовок в сессию

excel_file = 'Рейтинг клиник.xlsx'
xl = pd.ExcelFile(excel_file)
sheets = xl.sheet_names


def get_response(url):
    response = session.get(url=url)
    if response.status_code != 200:
        print("Произошла ошибка запроса, код:", response.status_code)
        print(response.reason)
    return response


def get_content(response, tag, parameter, name):
    soup = BeautifulSoup(response.text, "lxml")
    content = soup.find(tag, {parameter: name}).contents
    return content


def rating(data, tag1, tag2, parameter1, parameter2, name1, name2):
    soup = BeautifulSoup(data, 'html.parser')
    rate = soup.find(tag1, {parameter1: name1}).contents
    ratings = soup.find(tag2, {parameter2: name2}).contents
    return rate, ratings


# Яндекс

print('Парсим Яндекс.Карты...')
df = pd.read_excel(excel_file, sheets[0])
df.rename(columns={'Unnamed: 1': 'link'}, inplace=True)
df_yandex = df[['Клиника', 'link']]
df_yandex.rename(columns={'Клиника': 'clinic'}, inplace=True)
df_yandex.drop(df.tail(4).index, inplace=True)
df_yandex['link'] = df_yandex['link'].str.strip()

links = df_yandex['link'].to_list()
yandex_ids = []
for link in links:
    yandex_ids.append(link.split('/')[6])

parsed = []
for id in tqdm(yandex_ids):
    parser = YandexParser(id)
    all_data = parser.parse()
    parsed.append(all_data)
    sleep(0.5)

rate = []
ratings = []
reviews = []
for i in range(len(parsed)):
    rate.append(parsed[i]['company_info']['rating'])
    ratings.append(parsed[i]['company_info']['count_rating'])
    reviews.append(len(parsed[i]['company_reviews']))

df_yandex['rate'] = rate
df_yandex['ratings'] = ratings
df_yandex['reviews'] = reviews

# 2GIS
print('Парсим 2GIS...')
df = pd.read_excel(excel_file, sheets[1])
df.head()
df_2gis = df[['Клиника', 'Ссылка']]
df_2gis.rename(columns={'Клиника': 'clinic', 'Ссылка': 'link'}, inplace=True)
df_2gis.drop(df.tail(4).index, inplace=True)
df_2gis.dropna(inplace=True)
df_2gis['link'] = df_2gis['link'].str.strip()

gis_urls = df_2gis['link'].to_list()
rate_number = []
rate_quantity = []

for url in tqdm(gis_urls):
    resp = get_response(url)
    content = get_content(resp, "div", "class", "_1pfef7u")
    rate, ratings = rating(str(content), "div", "div", "class", "class", "_y10azs", "_jspzdm")
    ratings = str(ratings[0]).split(' ')
    rate_number.append(float(str(rate[0])))
    rate_quantity.append(int(ratings[0]))
    sleep(randint(1, 3))

df_2gis['rate_number'] = rate_number
df_2gis['rate_quantity'] = rate_quantity

# Google
print('Парсим Google Maps...')
df = pd.read_excel(excel_file, sheets[2])
df.head()
df_google = df[['Клиника', 'Ссылка']]
df_google.rename(columns={'Клиника': 'clinic', 'Ссылка': 'link'}, inplace=True)
df_google.drop(df.tail(5).index, inplace=True)
df_google['link'] = df_google['link'].str.strip()


def get_contents_google(response):
    soup = BeautifulSoup(response.text, "lxml")
    info = soup.find("script", text=re.compile("Отзывов"))
    return info


google_urls = df_google['link'].to_list()
g_rate_number = []
g_rate_quantity = []
found = []

for url in tqdm(google_urls):
    resp = get_response(url)
    content = str(get_contents_google(resp))
    lookup = [i.start() for i in re.finditer('Отзывов', content)]
    piece = content[lookup[0]: lookup[0]+100]
    string = piece.replace('\\', '')
    string = string.replace(']', '')
    parsed_list = string.split(',')
    for item in parsed_list:
        try:
            found.append(float(item))
        except:
            pass
    sleep(randint(1, 3))

for i in range(len(found)):
    if i % 2 == 0:
        g_rate_number.append(found[i])
    else:
        g_rate_quantity.append(found[i])

df_google['rate'] = g_rate_number
df_google['ratings'] = g_rate_quantity
df_google['ratings'] = df_google['ratings'].astype(int)

# Другие
print('Парсим другие площадки (пока 3)...')
df = pd.read_excel(excel_file, sheets[3])
df_other = df[['Площадка', 'Клиника (при наличии)', 'Ссылка']]
df_other.rename(columns={'Клиника (при наличии)': 'clinic', 'Ссылка': 'link', 'Площадка': 'platform'}, inplace=True)
df_other.drop(df.tail(5).index, inplace=True)
df_other['link'] = df_other['link'].str.strip()


def extract_domain(url):
    # Define a regular expression pattern for extracting the domain
    pattern = r"(https?://)?(www\d?\.)?(?P<domain>[\w\.-]+\.\w+)(/\S*)?"
    # Use re.match to search for the pattern at the beginning of the URL
    match = re.match(pattern, url)
    # Check if a match is found
    if match:
        # Extract the domain from the named group "domain"
        domain = match.group("domain")
        return domain
    else:
        pass


other_urls = df_other['link'].dropna().to_list()

domains = []
for url in other_urls:
    domains.append(extract_domain(url))

domains = dict.fromkeys(domains)
domains = [x for x in domains if x is not None]
domains = dict.fromkeys(domains)

for key, value in domains.items():
    urls = []
    for url in other_urls:
        # print(extract_domain(url))
        if extract_domain(url) == key:
            urls.append(url)
        domains[key] = urls

zoon_rate = []
zoon_ratings = []

for url in tqdm(domains['zoon.ru']):
    parsed = get_response(url)
    parse = get_content(parsed, "div", "class", "service-action__item")
    z_rate, z_ratings = rating(str(parse), "div", "a", "class", "class", "z-text--16 z-text--default z-text--bold",
                               "z-text--16 z-text--dark-gray js-toggle-content")
    zoon_rate.append(z_rate)
    string = str(z_ratings[0])
    string = string.strip('\n\t')
    z_rating = int(string.split('\xa0')[0])
    zoon_ratings.append(z_rating)
    sleep(randint(1, 3))

zoon_rate_fix = []

for item in zoon_rate:
    zoon_rate_fix.append(float(str(item[0]).replace(',', '.')))

df_otherlinks = pd.DataFrame.from_dict(domains, orient='index')
df_otherlinks = df_otherlinks.transpose()

zoonloc = df_otherlinks.columns.get_loc("zoon.ru")
df_otherlinks.insert(loc=zoonloc + 1, column='zoon.ru_rate', value=pd.Series(zoon_rate_fix))
df_otherlinks.insert(loc=zoonloc + 2, column='zoon.ru_ratings', value=pd.Series(zoon_ratings))

df_otherlinks = df_otherlinks.fillna(-1)
df_otherlinks['zoon.ru_ratings'] = df_otherlinks['zoon.ru_ratings'].astype(int)

pd_rates = []
pd_ratings = []

for url in tqdm(domains['prodoctorov.ru']):
    parsed = get_response(url)
    parse = get_content(parsed, "div", "id", "content")
    rate, ratings = rating(str(parse), "div", "span", "class", "class",
                           "ui-text ui-text_h5 ui-kit-color-text font-weight-medium mr-2", "b-box-rating__text")
    pd_rates.append(float(str(rate[0]).strip('\n ')))
    pd_ratings.append(int(str(ratings[0]).strip('\n ').split(' ')[0]))
    sleep(randint(1, 3))

pdloc = df_otherlinks.columns.get_loc("prodoctorov.ru")
df_otherlinks.insert(loc=pdloc + 1, column='prodoctorov.ru_rate', value=pd.Series(pd_rates))
df_otherlinks.insert(loc=pdloc + 2, column='prodoctorov.ru_ratings', value=pd.Series(pd_ratings))

sf_rates = []
sf_ratings = []

for url in tqdm(domains['msk.stom-firms.ru']):
    summ = 0
    response = get_response(url)
    parse = get_content(response, 'div', 'id', 'content')
    rate, ph = rating(str(parse), "span", "span", "class", "class",
                      "text__size--normal text__color--black text__style--bold viewBlock__realRaitingNumber",
                      "checkboxSelect__count")
    sf_rates.append(float(str(rate[0]).split(' ')[0]))
    soup = BeautifulSoup(str(parse), 'html.parser')
    test = soup.find_all("div", {'class': 'checkboxSelect__titleLabel'})
    if len(test) > 4:
        for i in range(len(test) - 2):
            summ += int(str(test[i].span.text))
    else:
        for i in range(len(test) - 1):
            summ += int(str(test[i].span.text))
    sf_ratings.append(summ)
    sleep(randint(1, 3))

sfloc = df_otherlinks.columns.get_loc("msk.stom-firms.ru")
df_otherlinks.insert(loc=sfloc + 1, column='msk.stom-firms.ru_rate', value=pd.Series(sf_rates))
df_otherlinks.insert(loc=sfloc + 2, column='msk.stom-firms.ru_ratings', value=pd.Series(sf_ratings))

# Объединение, вывод в файл
print('Записываем файл...')
df_other = df_otherlinks[['zoon.ru', 'zoon.ru_rate', 'zoon.ru_ratings',
                          'prodoctorov.ru', 'prodoctorov.ru_rate', 'prodoctorov.ru_ratings',
                          'msk.stom-firms.ru', 'msk.stom-firms.ru_rate', 'msk.stom-firms.ru_ratings']]
df_other.drop(df_other.tail(1).index, inplace=True)
df_other['prodoctorov.ru_ratings'] = df_other['prodoctorov.ru_ratings'].astype(int)
df_other['msk.stom-firms.ru_ratings'] = df_other['msk.stom-firms.ru_ratings'].astype(int)

df_yandex.rename(columns={'link': 'yandex_link'}, inplace=True)
df_google.rename(columns={'link': 'google_link'}, inplace=True)
df_2gis.rename(columns={'link': '2gis_link'}, inplace=True)

df_merged = pd.concat(
    objs=(iDF.set_index('clinic') for iDF in (df_yandex, df_google, df_2gis)),
    axis=1,
    join='inner'
).reset_index()

df_final = pd.concat([df_merged, df_other], axis=1)
date_str = strftime("%Y-%m-%d")
df_final.to_csv(f"parsed_{date_str}.csv", index=False)
