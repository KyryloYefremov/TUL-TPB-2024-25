import json
from pprint import pprint

import requests

from bs4 import BeautifulSoup

from cookies_parser import parse_cookie_file


URL = "https://www.idnes.cz/"


def parse_info(href: str, cookies: dict, links_to_parse: list[str]) -> dict | None:
    if href[:4] != 'http':
        return None

    # print(href)
    response = requests.get(href, cookies=cookies)

    soup = BeautifulSoup(response.text, 'html.parser')

    # In case of not valid page - skipping
    try:
        title = soup.find('h1').text
    except AttributeError:
        return None

    # Getting the whole text of the article
    text = ''
    for paragraph in soup.find_all('p'):
        text += paragraph.text + '\n'

    img_count = len(soup.find_all('img'))
    categories = [a.text for a in soup.find_all('a', class_='b')]

    try:
        date = soup.find('span', class_='time-date').get('content')
    except AttributeError:
        date = "NULL"

    li_tag = soup.find('li', class_='community-discusion')
    comments_num = int(li_tag.find('span').text.split()[0][1:]) if li_tag else 0

    result = {
        'title': title,
        'text': text,
        'img_count': img_count,
        'categories': categories,
        'date': date,
        'comments_num': comments_num,
        'link': href
    }

    articles_to_parse = soup.find_all('a', class_='art-link')
    links_to_parse += [article['href'] for article in articles_to_parse]

    return result


def main():
    cookies = parse_cookie_file('idnes_cookies.txt')
    response = requests.get(URL, cookies=cookies)

    soup = BeautifulSoup(response.text, 'html.parser')
    # with open('idnes.html', 'w', encoding='utf-8') as file:
    #     file.write(soup.prettify())

    # Parsing the main page: finding all articles
    articles_to_parse = soup.find_all('a', class_='art-link')
    if len(articles_to_parse) == 0:
        raise Exception('No articles found')
    links_to_parse = [article['href'] for article in articles_to_parse]

    # Parsing the articles
    outputs = []
    for link in links_to_parse:
        json_output = parse_info(href=link, cookies=cookies, links_to_parse=links_to_parse)

        # In case of not valid page - skipping
        if not json_output:
            continue

        outputs.append(json_output)
        print(len(outputs))
        if len(outputs) == 130000:
            break

    # Saving the parsed data to a file
    with open('idnes-data.json', 'w', encoding='utf-8') as file:
        json_to_write = json.dumps(outputs, ensure_ascii=False, indent=2)
        file.write(json_to_write)


if __name__ == '__main__':
    main()
