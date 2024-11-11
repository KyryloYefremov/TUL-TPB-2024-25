import json


def read_json(json_file: str) -> list[dict]:
    with open(json_file, 'r', encoding='utf-8') as file:
        return json.load(file)


def print_scraper_output_json(json_output):
    """
    Prints the first 5 articles from the json_output
    """
    for article in json_output[:5]:
        print('-' * 100 + '\n')
        print(
            f"Title: {article['title']}\n"
            f"Date: {article['date']}\n"
            f"Categories: {', '.join(article['categories'])}\n"
            f"Text: {article['text'][:100]}...\n"
            f"Image count: {article['img_count']}\n"
            f"Comments: {article['comments_num']}\n"
        )


def json_to_text_txt(json_file: str, txt_file: str):
    with open(json_file, 'r', encoding='utf-8') as infile:
        json_context = json.load(infile)

    with open(txt_file, 'w', encoding='utf-8') as outfile:
        for article in json_context:
            outfile.write('"' + article['title'] + '"' + '\n')
            outfile.write(article['text'] + '\n\n\n')


if __name__ == '__main__':
    # from scraper import OUTPUT_FILENAME
    # json_output = read_json(f"cv01/{OUTPUT_FILENAME}")
    # print_scraper_output_json(json_output=json_output)
    json_file = 'cv01/idnes-data250.json'
    txt_file = 'cv04/indes-data250.txt'
    json_to_text_txt(json_file, txt_file)
