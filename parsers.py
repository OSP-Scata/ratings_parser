import re

import requests
from bs4 import BeautifulSoup

headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0"}
session = requests.Session()  # Создаем сессию
session.headers = headers  # Передать заголовок в сессию


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


def get_response(url):
    response = session.get(url=url)
    if response.status_code != 200:
        print("Произошла ошибка запроса, код:", response.status_code)
        print(response.reason)
    return response.text


def get_content(response, tag, parameter, name):
    soup = BeautifulSoup(response, "html.parser")
    try:
        content = soup.find(tag, {parameter: name}).contents
        return content
    except:
        return None


def rating(data, tag1, tag2, parameter1, parameter2, name1, name2):
    soup = BeautifulSoup(str(data), 'html.parser')
    try:
        rate = soup.find(tag1, {parameter1: name1}).contents
        ratings = soup.find(tag2, {parameter2: name2}).contents
        return rate, ratings
    except:
        return None, None


def get_contents_google(response):
    soup = BeautifulSoup(response, "lxml")
    info = soup.find("script", text=re.compile("Отзывов"))
    return info
