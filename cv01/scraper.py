import json
import requests

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

from cookies_parser import parse_cookie_file


URL = "https://www.idnes.cz/"
OUTPUT_FILENAME = 'idnes-data.json'
MAX_THREADS = 20
ARTICLES_NUM = 150000


def parse_info(href: str, cookies: dict, links_to_parse: list[str], processed_links: set) -> dict | None:
    # Skip already processed links
    if href in processed_links:
        return None

    # Skip links that are not articles
    if not href.startswith('http'):
        return None

    try:
        response = requests.get(href, cookies=cookies)
        soup = BeautifulSoup(response.text, 'html.parser')

        # In case of not valid page - skipping
        try:
            title = soup.find('h1').text
        except AttributeError:
            return None
        if title == "Example Domain":
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

        # Find all new links to other articles on the page and add them to the main list
        new_links = {article['href'] for article in soup.find_all('a', class_='art-link')}
        links_to_parse += [link for link in new_links if link not in processed_links]

        return result

    except requests.RequestException as rexception:
        print(f"Request failed: {rexception}")
        return None


def main():
    cookies = parse_cookie_file('idnes_cookies.txt')
    response = requests.get(URL, cookies=cookies)

    soup = BeautifulSoup(response.text, 'html.parser')
    # with open('idnes.html', 'w', encoding='utf-8') as file:
    #     file.write(soup.prettify())

    # Parsing the main page: finding all articles and links to other articles
    articles_to_parse = [article['href'] for article in soup.find_all('a', class_='art-link')]
    if len(articles_to_parse) == 0:
        raise Exception('No articles found')

    # Parsing the articles
    outputs = []
    processed_links = set()

    while len(outputs) < ARTICLES_NUM:
        # Parsing the articles in parallel: use multiple threads (max_workers) to send requests
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Creating a list of tasks for the executor
            # submit() sends a single task to another thread and returns a Future object
            # that represents the result of the <parse_info> function
            futures = [executor.submit(
                parse_info, link, cookies, articles_to_parse, processed_links
            ) for link in articles_to_parse]

            for future in futures:
                json_result = future.result()
                if json_result:
                    outputs.append(json_result)
                    processed_links.add(json_result['link'])
                    print(len(outputs))
                    if len(outputs) >= ARTICLES_NUM:
                        print("Done\n" + "=" * 100)
                        with open(OUTPUT_FILENAME, 'w', encoding='utf-8', errors='replace') as file:
                            json_to_write = json.dumps(outputs, ensure_ascii=False, indent=2)
                            file.write(json_to_write)
                        exit(0)


if __name__ == '__main__':
    main()
