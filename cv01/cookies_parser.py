import re
import json
import pprint


def parse_cookie_file(cookiefile):
    """Parse a cookies.txt file and return a dictionary of key value pairs
    compatible with requests."""

    cookies = {}
    with open(cookiefile, 'r') as fp:
        for line in fp:
            if not re.match(r'^\#', line) and not line == "\n":
                line_fields = line.strip().split('\t')
                cookies[line_fields[5]] = line_fields[6]
    return cookies


if __name__ == '__main__':
    cookies = parse_cookie_file('idnes_cookies.txt')
    cookie_json = json.dumps(cookies)

    pprint.pprint(cookies)