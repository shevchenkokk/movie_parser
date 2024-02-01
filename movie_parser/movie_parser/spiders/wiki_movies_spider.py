import scrapy
from lxml import html
from movie_parser.items import MovieParserItem

class WikiMoviesSpider(scrapy.Spider):
    name = 'wiki_movies'
    start_urls = ['https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту']

    def remove_elements(self, _html):
        tree = html.fromstring(_html)
        for sup in tree.xpath("//sup"):
            sup.getparent().remove(sup)
        for span in tree.xpath("//span[@class='noprint']"):
            span.getparent().remove(span)
        return tree

    def parse(self, response):
        for href in response.xpath("//div[@class='mw-category-group']/ul/li/a/@href"):
            url = response.urljoin(href.extract())
            yield scrapy.Request(url, callback=self.parse_movie_page)

        yield from self.parse_next_page(response)

    def parse_next_page(self, response):
        next_page_html = response.xpath("//*[@id='mw-pages']/a[contains(text(), 'Следующая страница')]/@href").get()
        if next_page_html:
            next_page_url = response.urljoin(next_page_html)
            yield scrapy.Request(next_page_url, callback=self.parse)

    def parse_movie_page(self, response):
        item = MovieParserItem()
        td_title_html = response.xpath("//th[@class='infobox-above']").get()
        title = self.remove_elements(td_title_html).xpath("//text()")[-1]
        item['title'] = title.strip()

        td_genres_html = response.xpath("""
            //th[contains(a, 'Жанр') or contains(a, 'Жанры')]/following-sibling::td
        """).get()
        if td_genres_html:
            genres_text = self.remove_elements(td_genres_html).xpath("//text()")
            genres_text = [genre_text.strip().replace('/', ',') for genre_text in genres_text if genre_text.strip()]
            flattened_genres_text = []
            for genre_text in genres_text:
                flattened_genres_text.extend(genre_text.split(',') if len(genre_text) > 1 else genre_text)
            genres_text = flattened_genres_text
            genres, genre = [], ""
            for i, genre_text in enumerate(genres_text):
                if '-' in genre_text:
                    genre += genre_text.strip()
                    continue
                if ',' in genre_text:
                    genres.append(genre)
                    genre = ""
                    continue
                if i > 0 and ',' not in genres_text[i - 1] and '-' not in genres_text[i - 1]:
                    genres.append(genre)
                    genre = ""
                genre += genre_text.strip()
            genres.append(genre)
            item['genre'] = '\n'.join(genres)

        td_directors_html = response.xpath("""
            //th[contains(text(), 'Режиссёр') or contains(text(), 'Режиссёры')]/following-sibling::td
        """).get()
        if td_directors_html:
            directors = self.remove_elements(td_directors_html).xpath("//text()")
            directors = [director.strip() for director in directors if director.strip() and ',' not in director]
            item['director'] = '\n'.join(directors)
        
        td_countries_html = response.xpath("""
            //th[contains(text(), 'Страна') or contains(text(), 'Страны')]
            /following-sibling::td
        """).get()
        if td_countries_html:
            countries = self.remove_elements(td_countries_html).xpath("//text()")
            countries = [country.strip() for country in countries if country.strip() and ',' not in country]
            item['country'] = '\n'.join(countries)

        td_years_html = response.xpath("""
            //th[contains(text(), 'Год') or contains(text(), 'Первый показ')]
            /following-sibling::td
        """).get()
        if td_years_html:
            td_years = self.remove_elements(td_years_html)
            item['year'] = ' '.join(td_years.xpath("//text()")).strip()

        imdb_html = response.xpath("//th[contains(a, 'IMDb')]/following-sibling::td//a/@href").get()
        if imdb_html:
            imdb_url = response.urljoin(imdb_html)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15'
            }
            request = scrapy.Request(imdb_url, headers=headers, callback=self.parse_imdb)
            request.meta['item'] = item
            yield request
        else:
            yield item

    def parse_imdb(self, response):
        item = response.meta['item']
        item['imdb_rating'] = response.xpath("//span[@class='ipc-btn__text']//span/text()").get()
        yield item