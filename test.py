import re


class test_class:
    x=5



if __name__ == '__main__':
    url = 'https://docs.google.com/spreadsheets/d/1qSwCiZ_TBkIXGF0xsOzlrV9MUXa7vcY5AkUNjCckGhM/edit#gid=4209465'
    getid = '^.*/d/(.*)/.*$'
    pattern = re.compile(getid, re.IGNORECASE)
    x = pattern.findall(url)
    print(x)