import json
import requests

from bs4 import BeautifulSoup

from cookies_parser import parse_cookie_file


URL = "https://www.idnes.cz/"


def parse_info(article: BeautifulSoup, cookies: dict):
    link = article['href']
    print(link)
    response = requests.get(link, cookies=cookies)

    soup = BeautifulSoup(response.text, 'html.parser')

    with open('idnes_article.html', 'w', encoding='utf-8') as file:
        file.write(soup.prettify())

    title = soup.find('h1').text

    text = ''
    for paragraph in soup.find_all('p'):
        text += paragraph.text + '\n'

    img_count = len(soup.find_all('img'))
    categories = [a.text for a in soup.find_all('a', class_='b')]

    try:
        date = soup.find('span', class_='time-date').text
    except AttributeError:
        date = "NULL"

    li_tag = soup.find('li', class_='community-discusion')
    comments_num = li_tag.find('span').text.split()[1:] if li_tag else 0

    result = {
        'title': title,
        'text': text,
        'img_count': img_count,
        'categories': categories,
        'date': date,
        'comments_num': comments_num,
        'link': link
    }

    return result


def main():
    cookies = parse_cookie_file('idnes_cookies.txt')
    response = requests.get(URL, cookies=cookies)

    soup = BeautifulSoup(response.text, 'html.parser')
    with open('idnes.html', 'w', encoding='utf-8') as file:
        file.write(soup.prettify())

    # Parsing the main page: finding all articles
    articles = soup.find_all('a', class_='art-link')
    print(f"Pocet clanku: {len(articles)}")
    if len(articles) == 0:
        raise Exception('No articles found')

    # Parsing the articles
    outputs = []
    for article in articles:
        json_output = parse_info(article, cookies)
        # print(json_output)
        # print('-----------------------------------')
        outputs.append(json_output)
        break

    # Saving the parsed data to a file
    with open('idnes-data.json', 'w', encoding='utf-8') as file:
        json_to_write = json.dumps(outputs, ensure_ascii=False, indent=2)
        file.write(json_to_write)


if __name__ == '__main__':
    main()
