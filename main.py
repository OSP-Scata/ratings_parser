# Начало: импорты, функции, глобальные переменные

import warnings
from functools import reduce
from pathlib import Path
from random import randint
from shutil import rmtree
from time import sleep, strftime

from tqdm import tqdm
from yandex_reviews_parser.utils import YandexParser

from helper import *
from parsers import *

warnings.filterwarnings('ignore')

# Яндекс
df_yandex = prepare_dataframe(excel_file, sheets[0], 'Unnamed: 1', 'link', 'Клиника', 'clinic')
dict_clinics = {}
#dict_clinics = {df_yandex['clinic'].str.replace(r'ЮС-\d+', '', regex=True).str.strip(): df_yandex['clinic']}
for i in range(len(df_yandex['clinic'])):
    dict_clinics[df_yandex['clinic'][i]] = re.sub(r'ЮС-\d+', '', df_yandex['clinic'][i]).strip()
#print(dict_clinics)
# df_yandex['clinic'] = df_yandex['clinic'].str.replace(r'ЮС-\d+', '', regex=True).str.strip()
links = df_yandex['link'].to_list()
yandex_ids = []
for link in links:
    yandex_ids.append(link.split('/')[6])

ya = files_checking('workfiles/temp/yandex.csv')

if ya is None:
    parsed = []
    rate = []
    ratings = []
    reviews = []
    for id in tqdm(yandex_ids):
        parser = YandexParser(id)
        all_data = parser.parse()
        parsed.append(all_data)
        sleep(0.5)
    for i in range(len(parsed)):
        try:
            rate.append(parsed[i]['company_info']['rating'])
            ratings.append(parsed[i]['company_info']['count_rating'])
            reviews.append(len(parsed[i]['company_reviews']))
        except:
            rate.append(-1)
            ratings.append(-1)
            reviews.append(-1)
    df_yandex['yandex_rate'] = rate
    df_yandex['yandex_ratings'] = ratings
    df_yandex['yandex_reviews'] = reviews
    df_yandex.rename(columns={'link': 'yandex_link'}, inplace=True)
    df_yandex.to_csv('workfiles/temp/yandex.csv', index=False)
else:
    df_yandex = ya

# 2GIS
df_2gis = prepare_dataframe(excel_file, sheets[1], 'Клиника', 'clinic', 'Ссылка', 'link')
# df_2gis['clinic'] = df_2gis['clinic'].str.replace(r'ЮС-\d+', '', regex=True).str.strip()
gis_urls = df_2gis['link'].to_list()

gis = files_checking('workfiles/temp/2gis.csv')
if gis is None:
    rate_number = []
    rate_quantity = []
    for url in tqdm(gis_urls):
        if url:
            try:
                resp = get_response(url)
                content = get_content(resp, "div", "class", "_1pfef7u")
                rate, ratings = rating(content, "div", "div", "class", "class", "_y10azs",
                                       "_jspzdm")
                try:
                    ratings = str(ratings[0]).split(' ')
                    rate_number.append(float(str(rate[0])))
                    rate_quantity.append(int(ratings[0]))
                except:
                    rate_number.append(-1)
                    rate_quantity.append(-1)
            except:
                pass
        sleep(randint(1, 3))
    df_2gis['2gis_rate_number'] = pd.Series(rate_number)
    df_2gis['2gis_rate_quantity'] = pd.Series(rate_quantity)
    df_2gis.rename(columns={'link': '2gis_link'}, inplace=True)
    df_2gis.to_csv('workfiles/temp/2gis.csv', index=False)
else:
    df_2gis = gis

# Google
df_google = prepare_dataframe(excel_file, sheets[2], 'Клиника', 'clinic', 'Ссылка', 'link')
# df_google['clinic'] = df_google['clinic'].str.replace(r'ЮС-\d+', '', regex=True).str.strip()
google_urls = df_google['link'].to_list()
google = files_checking('workfiles/temp/google.csv')
if google is None:
    g_rate_number = []
    g_rate_quantity = []
    found = []

    for url in tqdm(google_urls):
        resp = get_response(url)
        content = str(get_contents_google(resp))
        lookup = [i.start() for i in re.finditer('Отзывов', content)]
        piece = content[lookup[0]: lookup[0] + 100]
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

    df_google['google_rate'] = g_rate_number
    df_google['google_ratings'] = g_rate_quantity
    df_google['google_ratings'] = df_google['google_ratings'].astype(int)
    df_google.rename(columns={'link': 'google_link'}, inplace=True)
    df_google.to_csv('workfiles/temp/google.csv', index=False)
else:
    df_google = google

# Другие
df_other = prepare_dataframe(excel_file, sheets[3], 'Площадка', 'platform', 'Клиника (при наличии)',
                             'clinic', 'Ссылка', 'link')
for key, value in dict_clinics.items():
    for i in range(len(df_other['clinic'])):
        if df_other['clinic'][i] == value:
            df_other['clinic'][i] = key
        else:
            pass
df_other['platform'].ffill(inplace=True)
df_other['platform'] = df_other['platform'].str.lower().str.strip()
platforms = df_other['platform'].unique()
dict_platforms = {}
for i in range(len(platforms)):
    dict_platforms[platforms[i]] = df_other[df_other['platform'] == platforms[i]]

# Zoon.ru
zoon = files_checking('workfiles/temp/zoon.csv')
if zoon is None:
    zoon_rate = []
    zoon_ratings = []
    dict_platforms['zoon.ru'].rename(columns={'link': 'zoon_link'}, inplace=True)
    for url in tqdm(dict_platforms['zoon.ru']['zoon_link']):
        parsed = get_response(url)
        parse = get_content(parsed, "div", "class", "service-action__item")
        z_rate, z_ratings = rating(parse, "div", "span", "class", "class",
                                   "z-text--16 z-text--default z-text--bold", None)
        zoon_rate.append(z_rate)
        string = str(z_ratings[0])
        string = string.strip('\n\t')
        z_rating = int(string.split('\xa0')[0])
        zoon_ratings.append(z_rating)
        sleep(randint(1, 3))

    zoon_rate_fix = []
    for item in zoon_rate:
        zoon_rate_fix.append(float(str(item[0]).replace(',', '.')))

    dict_platforms['zoon.ru']['zoon.ru_rate'] = zoon_rate_fix
    dict_platforms['zoon.ru']['zoon.ru_ratings'] = zoon_ratings
    dict_platforms['zoon.ru'] = dict_platforms['zoon.ru'].fillna(-1)
    dict_platforms['zoon.ru']['zoon.ru_ratings'] = dict_platforms['zoon.ru']['zoon.ru_ratings'].astype(int)
    dict_platforms['zoon.ru'].to_csv('workfiles/temp/zoon.csv', index=False)
else:
    dict_platforms['zoon.ru'] = zoon

# Prodoctorov.ru
prodoctorov = files_checking('workfiles/temp/prodoctorov.csv')
if prodoctorov is None:
    pd_rates = []
    pd_ratings = []
    dict_platforms['prodoctorov.ru'].rename(columns={'link': 'prodoctorov_link'}, inplace=True)
    for url in tqdm(dict_platforms['prodoctorov.ru']['prodoctorov_link']):
        parsed = get_response(url)
        parse = get_content(str(parsed), "div", "id", "content")
        rate, ratings = rating(parse, "div", "span", "class", "class",
                               "ui-text ui-text_h5 ui-kit-color-text font-weight-medium mr-2",
                               "b-box-rating__text")
        try:
            pd_rates.append(float(str(rate[0]).strip('\n ')))
            pd_ratings.append(int(str(ratings[0]).strip('\n ').split(' ')[0]))
        except:
            pd_rates.append(-1)
            pd_ratings.append(-1)
        sleep(randint(1, 3))
    dict_platforms['prodoctorov.ru']['prodoctorov.ru_rate'] = pd_rates
    dict_platforms['prodoctorov.ru']['prodoctorov.ru_ratings'] = pd_ratings
    dict_platforms['prodoctorov.ru'].to_csv('workfiles/temp/prodoctorov.csv', index=False)
else:
    dict_platforms['prodoctorov.ru'] = prodoctorov

# Msk.stom-firms.ru
stomfirms = files_checking('workfiles/temp/msk_stom-firms.csv')
if stomfirms is None:
    sf_rates = []
    sf_ratings = []
    dict_platforms['msk.stom-firms.ru'].rename(columns={'link': 'msk.stom-firms_link'}, inplace=True)
    for url in tqdm(dict_platforms['msk.stom-firms.ru']['msk.stom-firms_link']):
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
    dict_platforms['msk.stom-firms.ru']['msk.stom-firms.ru_rate'] = sf_rates
    dict_platforms['msk.stom-firms.ru']['msk.stom-firms.ru_ratings'] = sf_ratings
    dict_platforms['msk.stom-firms.ru'].to_csv('workfiles/temp/msk_stom-firms.csv', index=False)
else:
    dict_platforms['msk.stom-firms.ru'] = stomfirms

# Doctu.ru
doctu = files_checking('workfiles/temp/doctu.csv')
if doctu is None:
    doctu_rates = []
    doctu_ratings = []
    dict_platforms['doctu.ru'].rename(columns={'link': 'doctu_link'}, inplace=True)
    for url in tqdm(dict_platforms['doctu.ru']['doctu_link']):
        try:
            doctu_parse = selenium_parsing(url, "div", "itemprop", "aggregateRating")
            rate = BeautifulSoup(str(doctu_parse[4]), 'html.parser').get_text()
            reviews = BeautifulSoup(str(doctu_parse[-2]), 'html.parser').get_text()
            doctu_rates.append(float(rate))
            doctu_ratings.append(int(reviews.split(' ')[0]))
        except:
            doctu_rates.append(-1)
            doctu_ratings.append(-1)
    dict_platforms['doctu.ru']['doctu.ru_rate'] = doctu_rates
    dict_platforms['doctu.ru']['doctu.ru_ratings'] = doctu_ratings
    dict_platforms['doctu.ru'].to_csv('workfiles/temp/doctu.csv', index=False)
else:
    dict_platforms['doctu.ru'] = doctu

# napopravku.ru
dict_platforms['napopravku.ru'].rename(columns={'link': 'napopravku_link'}, inplace=True)
napopravku = files_checking('workfiles/temp/napopravku.csv')
if napopravku is None:
    np_rates = []
    np_ratings = []
    np_reviews = []
    for url in tqdm(dict_platforms['napopravku.ru']['napopravku_link']):
        np_parse = selenium_parsing(url, "div", "class", "clinic-title__rating-info rating-info")
        if np_parse is not None:
            rate = BeautifulSoup(str(np_parse[0]), 'html.parser').get_text()
            reviews = BeautifulSoup(str(np_parse[1]), 'html.parser').get_text()
            np_rates.append(rate)
            try:
                np_ratings.append(reviews.split(' ')[-2])
            except:
                np_ratings.append(-1)
            try:
                np_reviews.append(reviews.split(' ')[2])
            except:
                np_reviews.append(-1)
        else:
            np_rates.append(-1)
            np_ratings.append(-1)
            np_reviews.append(-1)
    dict_platforms['napopravku.ru']['napopravku.ru_rate'] = np_rates
    dict_platforms['napopravku.ru']['napopravku.ru_ratings'] = np_ratings
    dict_platforms['napopravku.ru']['napopravku.ru_reviews'] = np_reviews
    dict_platforms['napopravku.ru'].to_csv('workfiles/temp/napopravku.csv', index=False)
else:
    dict_platforms['napopravku.ru'] = napopravku

# Topdent
topdent = files_checking('workfiles/temp/topdent.csv')
if topdent is None:
    td_rates = []
    td_ratings = []
    dict_platforms['topdent.ru'].rename(columns={'link': 'topdent_link'}, inplace=True)
    for url in tqdm(dict_platforms['topdent.ru']['topdent_link']):
        response = get_response(url)
        parse = get_content(response, 'span', 'class', 'rate')
        rate, ratings = rating(parse, "span", "span", "class", "class",
                               "rate__value", "rate__count")
        td_rates.append(float(str(rate[0])))
        td_ratings.append(int(str(ratings[0]).split(' ')[0]))
        sleep(randint(1, 3))
    dict_platforms['topdent.ru']['topdent.ru_rate'] = td_rates
    dict_platforms['topdent.ru']['topdent.ru_ratings'] = td_ratings
    dict_platforms['topdent.ru'].to_csv('workfiles/temp/topdent.csv', index=False)
else:
    dict_platforms['topdent.ru'] = topdent

# stomdoc
stomdoc = files_checking('workfiles/temp/stomdoc.csv')
if stomdoc is None:
    sd_rates = []
    sd_ratings = []
    dict_platforms['stomdoc.ru'].rename(columns={'link': 'stomdoc_link'}, inplace=True)
    for url in tqdm(dict_platforms['stomdoc.ru']['stomdoc_link']):
        try:
            response = get_response(url)
            rate = get_content(response, 'div', 'class', 'b-clinic_page_heading_rating_wg_num')
            r_content = get_content(response, 'div', 'class', 'col-xs-24 col-sm-14 col-md-17 col-lg-19 '
                                                              'col-sm-vertical-middle col-md-vertical-middle col-lg-vertical-middle')
            ratings = BeautifulSoup(str(r_content[1]), 'html.parser').get_text()
            sd_rates.append(float(rate[0]))
            sd_ratings.append(int(ratings.split(' ')[1]))
        except:
            sd_rates.append(-1)
            sd_ratings.append(-1)
        sleep(randint(1, 3))
    dict_platforms['stomdoc.ru']['stomdoc.ru_rate'] = sd_rates
    dict_platforms['stomdoc.ru']['stomdoc.ru_ratings'] = sd_ratings
    dict_platforms['stomdoc.ru'].to_csv('workfiles/temp/stomdoc.csv', index=False)
else:
    dict_platforms['stomdoc.ru'] = stomdoc

# 32top
top32 = files_checking('workfiles/temp/32top.csv')
if top32 is None:
    top32_rates = []
    top32_ratings = []
    dict_platforms['32top.ru'].rename(columns={'link': '32top_link'}, inplace=True)
    for url in tqdm(dict_platforms['32top.ru']['32top_link']):
        try:
            response = get_response(url)
            parse = get_content(response, 'div', 'itemprop', 'aggregateRating')
            rate = BeautifulSoup(str(parse[1]), 'html.parser').find("meta")
            ratings = BeautifulSoup(str(parse[3]), 'html.parser').find("meta")
            top32_rates.append(rate['content'])
            top32_ratings.append(ratings['content'])
        except:
            top32_rates.append(-1)
            top32_ratings.append(-1)
        sleep(randint(1, 3))
    dict_platforms['32top.ru']['32top.ru_rate'] = top32_rates
    dict_platforms['32top.ru']['32top.ru_ratings'] = top32_ratings
    dict_platforms['32top.ru'].to_csv('workfiles/temp/32top.csv', index=False)
else:
    dict_platforms['32top.ru'] = top32

# flamp
flamp = files_checking('workfiles/temp/flamp.csv')
if flamp is None:
    flamp_rates = []
    flamp_ratings = []
    dict_platforms['flamp.ru'].rename(columns={'link': 'flamp_link'}, inplace=True)
    for url in tqdm(dict_platforms['flamp.ru']['flamp_link']):
        try:
            response = get_response(url)
            parse = get_content(response, 'div', 'itemprop', 'aggregateRating')
            rate = BeautifulSoup(str(parse[3]), 'html.parser').find("meta")
            ratings = BeautifulSoup(str(parse[1]), 'html.parser').find("meta")
            flamp_rates.append(rate['content'])
            flamp_ratings.append(ratings['content'])
        except:
            flamp_rates.append(-1)
            flamp_ratings.append(-1)
        sleep(randint(1, 3))
    dict_platforms['flamp.ru']['flamp.ru_rate'] = flamp_rates
    dict_platforms['flamp.ru']['flamp.ru_ratings'] = flamp_ratings
    dict_platforms['flamp.ru'].to_csv('workfiles/temp/flamp.csv', index=False)
else:
    dict_platforms['flamp.ru'] = flamp

# Объединение, вывод в файл
print('Записываем файл...')
date_str = strftime("%Y-%m-%d")
keys = ['zoon.ru', 'prodoctorov.ru', 'msk.stom-firms.ru', 'doctu.ru', 'napopravku.ru',
        'topdent.ru', 'stomdoc.ru', '32top.ru', 'flamp.ru']
dict_other = {key: dict_platforms[key] for key in keys}
for value in dict_other.values():
    value.fillna(-1, inplace=True)
    value.drop('platform', axis=1, inplace=True)
dict_other['prodoctorov.ru']['prodoctorov.ru_ratings'] = (dict_other['prodoctorov.ru']['prodoctorov.ru_ratings']
                                                          .astype(int))
dict_other['msk.stom-firms.ru']['msk.stom-firms.ru_ratings'] = (
    dict_other['msk.stom-firms.ru']['msk.stom-firms.ru_ratings']
    .astype(int))

df_oth = list(dict_other.values())  # делаем список из элементов словаря
df_merged = [df_yandex, df_google, df_2gis, df_oth]  # список из датафреймов и одного вложенного списка
chained = []  # раскрываем вложенный список на 1 уровень
for item in df_merged:
    if isinstance(item, list):
        for subitem in item:
            chained.append(subitem)
    else:
        chained.append(item)
# объединение датафреймов из списка по столбцу клиники
df_final = reduce(lambda left, right: pd.merge(left, right, on='clinic',
                                               how='outer', suffixes=(None, None)), chained)
df_final.to_csv(f'workfiles/parsed_{date_str}.csv', index=False)
is_final = files_checking(f'workfiles/parsed_{date_str}.csv')
if is_final is not None:
    for path in Path('workfiles/temp/').glob('*'):
        if path.is_dir():
            rmtree(path)
        else:
            path.unlink()
