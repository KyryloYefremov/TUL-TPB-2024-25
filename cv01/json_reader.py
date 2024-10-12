import json

from scraper import OUTPUT_FILENAME


def read_json(json_file: str) -> list[dict]:
    with open(json_file, 'r', encoding='utf-8') as file:
        return json.load(file)


def print_scraper_output_json(json_output):
    """
    Prints the first 5 articles from the json_output
    """
    for article in json_output[:5]:
        print('-' * 200 + '\n')
        print(
            f"Title: {article['title']}\n"
            f"Date: {article['date']}\n"
            f"Categories: {', '.join(article['categories'])}\n"
            f"Text: {article['text'][:100]}...\n"
            f"Image count: {article['img_count']}\n"
            f"Comments: {article['comments_num']}\n"
        )


if __name__ == '__main__':
    json_output = read_json(OUTPUT_FILENAME)
    print_scraper_output_json(json_output=json_output)

