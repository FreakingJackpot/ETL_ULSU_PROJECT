from bs4 import BeautifulSoup
import requests
import re


class Parser:

    data_start = []
    data_end = []
    url_list = []
    dictionary_list = []
    dictionary = {}

    def __init__(self):

        self.set_url_list("https://xn--90aivcdt6dxbc.xn--p1ai/stopkoronavirus/")
        td = self.parser_url_list()

    def parser_url_list(self):

        for i in range(len(self.url_list)):
            url_domen = "https://xn--90aivcdt6dxbc.xn--p1ai/"
            req = requests.get(url_domen + self.url_list[i])
            src = req.text
            soup = BeautifulSoup(src, 'lxml')
            detail__body = soup.find("div", class_="article-detail__body")
            tbody = detail__body.find("tbody")
            td = tbody.find_all("td")

            self.set_data(detail__body)
            td = self.clearing(td)
            self.set_dict(td)

    def set_url_list(self, url):

        req = requests.get(url)
        src = req.text
        soup = BeautifulSoup(src, 'lxml')
        media_page = soup.find("body")
        material_cards = media_page.find_all("a", class_="u-material-card u-material-cards__card")

        for link in material_cards:
            self.url_list.append(str(link.get('href')))

        for elem in self.url_list:
            if elem.find("v-rossii-za-nedelyu-vyzdorovelo-") == -1:
                self.url_list.remove(elem)

    def clearing(self, td):

        for i in range(len(td)):
            td[i] = str(td[i])

        for i in range(len(td)):
            td[i] = re.sub('<.*?>', '', td[i])
            td[i] = re.sub('\\n', '', td[i])
            td[i] = re.sub('\\t', '', td[i])
            td[i] = re.sub('\\r', '', td[i])
        return td

    def set_dict(self, td):

        hospitalized = td[1]
        recovered = td[2]
        revealed = td[3]
        died = td[4]
        self.dictionary = {}
        for i in range(5, len(td), 5):
            self.dictionary[td[i]] = {hospitalized: td[i + 1], recovered: td[i + 2], revealed: td[i + 3], died: td[i + 4]}

        self.dictionary_list.append(self.dictionary)

    def set_data(self, detail__body):

        data = str(detail__body.find("h3"))
        data = re.sub('<.*?>', '', data)
        matches = re.findall(r"\d+\.?\d*- \d+\.?\d*", data)
        dates = matches[0].split('-')
        self.data_start.append(dates[0])
        self.data_end.append(dates[1])

    def get_summary_all(self):
        return [(x, y, z) for x, y, z in zip(self.data_start, self.data_end, self.dictionary_list)]

    def get_summary_past(self):
         return self.data_start[0], self.data_end[0], self.dictionary_list[0]

    def __getattr__(self, dictionary_list):
        return self.dictionary_list

    def __getattr__(self, url_list):
        return self.url_list

    def __getattr__(self, data_start):
        return self.data_start

    def __getattr__(self, data_end):
        return self.data_end


if __name__ == '__main__':

    a = Parser()
    print(*a.get_summary_all(), sep="\n")
    print(a.get_summary_past())


