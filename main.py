# Начало: импорты, функции, глобальные переменные

import warnings
from random import randint
from time import sleep, strftime

import pandas as pd
from tqdm import tqdm
from yandex_reviews_parser.utils import YandexParser

from parsers import *

warnings.filterwarnings('ignore')

excel_file = 'workfiles/Внутр_Рейтинг клиник.xlsx'
xl = pd.ExcelFile(excel_file)
sheets = xl.sheet_names


def prepare_dataframe(file, sheet, field1, field2, field3, field4, field5=None, field6=None):
    print('Парсим', sheet)
    df = pd.read_excel(file, sheet_name=sheet)
    df.rename(columns={field1: field2}, inplace=True)
    df.rename(columns={field3: field4}, inplace=True)
    if field5 is not None and field6 is not None:
        df.rename(columns={field5: field6}, inplace=True)
        df = df[[field2, field4, field6]]
    else:
        df = df[[field2, field4]]

    df.drop(df.tail(4).index, inplace=True)
    df[field4] = df[field4].str.strip()
    if sheet != sheets[3]:
        df.dropna(inplace=True)
    return df


# Яндекс
df_yandex = prepare_dataframe(excel_file, sheets[0], 'Unnamed: 1', 'link', 'Клиника', 'clinic')
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
    try:
        rate.append(parsed[i]['company_info']['rating'])
        ratings.append(parsed[i]['company_info']['count_rating'])
        reviews.append(len(parsed[i]['company_reviews']))
    except KeyError:
        rate.append(-1)
        ratings.append(-1)
        reviews.append(-1)

df_yandex['rate'] = rate
df_yandex['ratings'] = ratings
df_yandex['reviews'] = reviews

# 2GIS
df_2gis = prepare_dataframe(excel_file, sheets[1], 'Клиника', 'clinic', 'Ссылка', 'link')
gis_urls = df_2gis['link'].to_list()
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

df_2gis['rate_number'] = pd.Series(rate_number)
df_2gis['rate_quantity'] = pd.Series(rate_quantity)

# Google
df_google = prepare_dataframe(excel_file, sheets[2], 'Клиника', 'clinic', 'Ссылка', 'link')

google_urls = df_google['link'].to_list()
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

df_google['rate'] = g_rate_number
df_google['ratings'] = g_rate_quantity
df_google['ratings'] = df_google['ratings'].astype(int)

# Другие
df_other = prepare_dataframe(excel_file, sheets[3], 'Площадка', 'platform', 'Клиника (при наличии)',
                             'clinic', 'Ссылка', 'link')
other_urls = df_other['link'].dropna().to_list()

# выдёргиваем домены
domains = []
for url in other_urls:
    domains.append(extract_domain(url))

domains = dict.fromkeys(domains)
domains = [x for x in domains if x is not None]
domains = dict.fromkeys(domains)

for key, value in domains.items():
    urls = []
    for url in other_urls:
        if extract_domain(url) == key:
            urls.append(url)
        domains[key] = urls
# print(domains) # для проверки, что правильно выдернулось

# Zoon.ru
zoon_rate = []
zoon_ratings = []

for url in tqdm(domains['zoon.ru']):
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

df_otherlinks = pd.DataFrame.from_dict(domains, orient='index')
df_otherlinks = df_otherlinks.transpose()

zoonloc = df_otherlinks.columns.get_loc("zoon.ru")
df_otherlinks.insert(loc=zoonloc + 1, column='zoon.ru_rate', value=pd.Series(zoon_rate_fix))
df_otherlinks.insert(loc=zoonloc + 2, column='zoon.ru_ratings', value=pd.Series(zoon_ratings))

df_otherlinks = df_otherlinks.fillna(-1)
df_otherlinks['zoon.ru_ratings'] = df_otherlinks['zoon.ru_ratings'].astype(int)

# Prodoctorov.ru
pd_rates = []
pd_ratings = []

for url in tqdm(domains['prodoctorov.ru']):
    parsed = get_response(url)
    parse = get_content(str(parsed), "div", "id", "content")
    rate, ratings = rating(parse, "div", "span", "class", "class",
                           "ui-text ui-text_h5 ui-kit-color-text font-weight-medium mr-2", "b-box-rating__text")
    pd_rates.append(float(str(rate[0]).strip('\n ')))
    pd_ratings.append(int(str(ratings[0]).strip('\n ').split(' ')[0]))
    sleep(randint(1, 3))

pdloc = df_otherlinks.columns.get_loc("prodoctorov.ru")
df_otherlinks.insert(loc=pdloc + 1, column='prodoctorov.ru_rate', value=pd.Series(pd_rates))
df_otherlinks.insert(loc=pdloc + 2, column='prodoctorov.ru_ratings', value=pd.Series(pd_ratings))

# Msk.stom-firms.ru
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

# Doctu.ru
doctu_rates = []
doctu_ratings = []

for url in tqdm(domains['doctu.ru']):
    doctu_parse = []
    doctu_parse = selenium_parsing(url, "div", "itemprop", "aggregateRating")
    rate = BeautifulSoup(str(doctu_parse[4]), 'html.parser').get_text()
    reviews = BeautifulSoup(str(doctu_parse[-2]), 'html.parser').get_text()
    doctu_rates.append(float(rate))
    doctu_ratings.append(int(reviews.split(' ')[0]))

doctuloc = df_otherlinks.columns.get_loc("doctu.ru")
df_otherlinks.insert(loc=doctuloc + 1, column='doctu.ru_rate', value=pd.Series(doctu_rates))
df_otherlinks.insert(loc=doctuloc + 2, column='doctu.ru_ratings', value=pd.Series(doctu_ratings))

# napopravku.ru
np_rates = []
np_ratings = []
np_reviews = []
for url in tqdm(domains['napopravku.ru']):
    np_parse = selenium_parsing(url, "div", "class", "clinic-title__rating-info rating-info")
    try:
        rate = BeautifulSoup(str(np_parse[0]), 'html.parser').get_text()
        reviews = BeautifulSoup(str(np_parse[1]), 'html.parser').get_text()
        np_rates.append(rate)
        np_ratings.append(reviews.split(' ')[-2])
        np_reviews.append(reviews.split(' ')[2])
    except:
        np_rates.append(-1)
        np_ratings.append(-1)
        np_reviews.append(-1)

nploc = df_otherlinks.columns.get_loc("napopravku.ru")
df_otherlinks.insert(loc=nploc + 1, column='napopravku.ru_rate', value=pd.Series(np_rates))
df_otherlinks.insert(loc=nploc + 2, column='napopravku.ru_ratings', value=pd.Series(np_ratings))
df_otherlinks.insert(loc=nploc + 3, column='napopravku.ru_reviews', value=pd.Series(np_reviews))

# Topdent
td_rates = []
td_ratings = []

for url in tqdm(domains['topdent.ru']):
    response = get_response(url)
    parse = get_content(response, 'span', 'class', 'rate')
    rate, ratings = rating(parse, "span", "span", "class", "class",
                           "rate__value", "rate__count")
    td_rates.append(float(str(rate[0])))
    td_ratings.append(int(str(ratings[0]).split(' ')[0]))
    sleep(randint(1, 3))

tdloc = df_otherlinks.columns.get_loc("topdent.ru")
df_otherlinks.insert(loc=tdloc + 1, column='topdent.ru_rate', value=pd.Series(td_rates))
df_otherlinks.insert(loc=tdloc + 2, column='topdent.ru_ratings', value=pd.Series(td_ratings))

# stomdoc
sd_rates = []
sd_ratings = []

for url in tqdm(domains['stomdoc.ru']):
    response = get_response(url)
    rate = get_content(response, 'div', 'class', 'b-clinic_page_heading_rating_wg_num')
    r_content = get_content(response, 'div', 'class', 'col-xs-24 col-sm-14 col-md-17 col-lg-19 '
                                                      'col-sm-vertical-middle col-md-vertical-middle col-lg-vertical-middle')
    ratings = BeautifulSoup(str(r_content[1]), 'html.parser').get_text()
    sd_rates.append(float(rate[0]))
    sd_ratings.append(int(ratings.split(' ')[1]))
    sleep(randint(1, 3))

sdloc = df_otherlinks.columns.get_loc("stomdoc.ru")
df_otherlinks.insert(loc=sdloc + 1, column='stomdoc.ru_rate', value=pd.Series(sd_rates))
df_otherlinks.insert(loc=sdloc + 2, column='stomdoc.ru_ratings', value=pd.Series(sd_ratings))

# 32top
top32_rates = []
top32_ratings = []

for url in tqdm(domains['32top.ru']):
    response = get_response(url)
    parse = get_content(response, 'div', 'itemprop', 'aggregateRating')
    try:
        rate = BeautifulSoup(str(parse[1]), 'html.parser').find("meta")
        ratings = BeautifulSoup(str(parse[3]), 'html.parser').find("meta")
        top32_rates.append(rate['content'])
        top32_ratings.append(ratings['content'])
    except:
        top32_rates.append(-1)
        top32_ratings.append(-1)
    sleep(randint(1, 3))

top32loc = df_otherlinks.columns.get_loc("32top.ru")
df_otherlinks.insert(loc=top32loc + 1, column='32top.ru_rate', value=pd.Series(top32_rates))
df_otherlinks.insert(loc=top32loc + 2, column='32top.ru_ratings', value=pd.Series(top32_ratings))

# flamp
flamp_rates = []
flamp_ratings = []

for url in tqdm(domains['moscow.flamp.ru']):
    response = get_response(url)
    parse = get_content(response, 'div', 'itemprop', 'aggregateRating')
    try:
        rate = BeautifulSoup(str(parse[3]), 'html.parser').find("meta")
        ratings = BeautifulSoup(str(parse[1]), 'html.parser').find("meta")
        flamp_rates.append(rate['content'])
        flamp_ratings.append(ratings['content'])
    except:
        flamp_rates.append(-1)
        flamp_ratings.append(-1)
    sleep(randint(1, 3))

flamploc = df_otherlinks.columns.get_loc("moscow.flamp.ru")
df_otherlinks.insert(loc=flamploc + 1, column='moscow.flamp.ru_rate', value=pd.Series(flamp_rates))
df_otherlinks.insert(loc=flamploc + 2, column='moscow.flamp.ru_ratings', value=pd.Series(flamp_ratings))

# Объединение, вывод в файл
print('Записываем файл...')
df_other = df_otherlinks[['zoon.ru', 'zoon.ru_rate', 'zoon.ru_ratings',
                          'prodoctorov.ru', 'prodoctorov.ru_rate', 'prodoctorov.ru_ratings',
                          'msk.stom-firms.ru', 'msk.stom-firms.ru_rate', 'msk.stom-firms.ru_ratings',
                          'doctu.ru', 'doctu.ru_rate', 'doctu.ru_ratings',
                          'napopravku.ru', 'napopravku.ru_rate', 'napopravku.ru_ratings', 'napopravku.ru_reviews',
                          'topdent.ru', 'topdent.ru_rate', 'topdent.ru_ratings',
                          'stomdoc.ru', 'stomdoc.ru_rate', 'stomdoc.ru_ratings',
                          '32top.ru', '32top.ru_rate', '32top.ru_ratings',
                          'moscow.flamp.ru', 'moscow.flamp.ru_rate', 'moscow.flamp.ru_ratings']]
# df_other.drop(df_other.tail(1).index, inplace=True)
df_other.fillna(-1, inplace=True)
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
df_final.to_csv(f'workfiles/parsed_{date_str}.csv', index=False)
