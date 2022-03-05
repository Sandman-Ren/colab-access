import abc
import time
import urllib
from typing import Dict, Union, List, Tuple
from urllib import request, parse

import os
import sys
from pathlib import Path
from os import PathLike

import base64

import json

import scrapy
import scrapy.crawler

SPOTIFY_API_BASE_URL = r'https://api.spotify.com/v1'
SPOTIFY_OAUTH2_URL = r'https://accounts.spotify.com/api/token'


class Token:
    pass


class SpotifyClientCredentialsToken(Token):
    AUTH_BODY = parse.urlencode([('grant_type', 'client_credentials')]).encode()
    SPOTIFY_SEARCH_ENDPOINT = r'/search'

    @staticmethod
    def from_json_file(file: Path) -> "SpotifyClientCredentialsToken":
        attributes: Dict = json.load(file.open('r'))
        token: SpotifyClientCredentialsToken
        try:
            client_id = attributes['client_id']
            client_secret = attributes['client_secret']
            token = SpotifyClientCredentialsToken(client_id, client_secret)
        except KeyError:
            raise ValueError("Expect field client_id and client_secret to be both present in the file.")
        return token

    def __init__(self, client_id: str = None, client_secret: str = None, lazy_initialize=True):

        if client_id is None:
            try:
                client_id = os.environ['CLIENT_ID']
            except KeyError:
                raise Exception("You must specify client_id as an argument or set the CLIENT_ID environment variable.")

            try:
                client_secret = os.environ['CLIENT_SECRET']
            except KeyError:
                raise Exception(
                    "You must specify client_secret as an agrument or set the CLIENT_SECRET environment variable.")

        self.client_id = client_id
        self.client_secret = client_secret
        self.valid_until = 0.0
        self.token_info: Dict[str, Union[str, int]] = None

        if not lazy_initialize:
            self.refresh_token()

    def refresh_token(self):
        refresh_time = time.time()
        self.token_info = self._get_auth_token()
        self.valid_until = refresh_time + self.token_info['expires_in'] - 30  # subtract some bleed

    def get_authorized_header(self) -> Dict[str, str]:
        """
        Assembles OAuth2 header with the token, retreived from the Spotify OAuth Service.
        :return: a dictionary representing the authentication header
        """
        if time.time() > self.valid_until:
            self.refresh_token()
        return {"Authorization": f"Bearer {self.token_info['access_token']}"}

    def _get_auth_token(self):
        auth_header = self._assemble_oauth2_header()
        req = request.Request(SPOTIFY_OAUTH2_URL, data=SpotifyClientCredentialsToken.AUTH_BODY, headers=auth_header,
                              method='POST')
        response = request.urlopen(req)
        return json.load(response)

    def _assemble_oauth2_header(self) -> Dict[str, str]:
        # encode client_id:client_secret in base64
        encoded_id_secret = base64.b64encode(f'{self.client_id}:{self.client_secret}'.encode()).decode()
        # assemble headers
        return {
            'Authorization': f'Basic {encoded_id_secret}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }


class Any:
    pass


class SpotifySearchCrawler(scrapy.Spider):
    name: str = "SpotifySearchCrawler"

    def __init__(self, client_id: str, client_secret: str, **kwargs):
        super().__init__(**kwargs)

        if client_id is None:
            try:
                client_id = os.environ['CLIENT_ID']
            except KeyError:
                raise Exception("You must specify client_id as an argument or set the CLIENT_ID environment variable.")

            try:
                client_secret = os.environ['CLIENT_SECRET']
            except KeyError:
                raise Exception(
                    "You must specify client_secret as an agrument or set the CLIENT_SECRET environment variable.")

        self.client_id = client_id
        self.client_secret = client_secret
        self.token = SpotifyClientCredentialsToken(client_id, client_secret)

        self.output_dir = Path.cwd() / f'crawler-{self.name}-output'
        if not self.output_dir.exists():
            self.output_dir.mkdir()

    def start_requests(self):
        search_type = 'track'
        target_genre = 'hip-hop'
        target_year = ['2019', '2020', '2021', '2022']
        pagination_limit = 50

        # assemble the starting url
        for year in target_year:
            query_string = parse.urlencode([
                ('q', f'genre:{target_genre} year:{year}'),
                ('type', search_type),
                ('market', 'US'),
                ('limit', pagination_limit),
                ('offset', 0)
            ])
            search_url = f'{SPOTIFY_API_BASE_URL}/search?{query_string}'

            self.logger.info(f'Search URL: {search_url}')

            yield self.get_authorized_request(search_url, callback=self.parse)

    def parse(self, response: scrapy.http.TextResponse, **kwargs):
        searched_tracks: dict = response.json()['tracks']
        yield searched_tracks
        if 'next' in searched_tracks:
            yield self.get_authorized_request(url=searched_tracks['next'], callback=self.parse)

    def get_authorized_request(self, url, callback=None) -> scrapy.Request:

        authorized_header = self.token.get_authorized_header()
        authorized_header['Content-Type'] = 'application/json'
        return scrapy.Request(url=url, headers=authorized_header, callback=callback)


class SearchedTrackItemPipeline:
    """
    Pipeline for processing items scraped from the SpotifySearchCrawler.
    """

    def __init__(self):
        self.output_dir = None
        self.logger = None

    def open_spider(self, spider: scrapy.Spider):
        # set the output directory based on the information
        self.output_dir: Path = getattr(spider, 'output_dir')
        self.logger = spider.logger
        self.logger.info(f'SearchedTrackItemPipeline output directory: {self.output_dir}')

    def process_item(self, item: Dict[str, Union[str, Any]], spider: scrapy.Spider) -> Dict[str, Any]:
        self.logger.info(f'Processing item: {item["href"]}')
        # collect some metadata to store these objects
        href: str = item['href']
        parsed_href = parse.urlparse(href)
        query_parameters = parsed_href.query  # we will use this as the filename

        tracks = item['items']

        # write to file
        output_filename: Path = self.output_dir / f'{query_parameters}.json'
        json.dump(tracks, output_filename.open(mode='w', encoding='utf-8'))

        return item


if __name__ == '__main__':
    s = SpotifySearchCrawler(client_id='41d11c64c99f4295b22b262d02a041ff',
                             client_secret='33c03b29679c45ada65a04e65f8b98f2')
    crawler_process = scrapy.crawler.CrawlerProcess(settings={
        'ITEM_PIPELINES': {
            'spotify.SearchedTrackItemPipeline': 800
        },
        'LOG_LEVEL': "INFO"
    })
    print(vars(crawler_process.settings))
    crawler_process.crawl(SpotifySearchCrawler, client_id='41d11c64c99f4295b22b262d02a041ff',
                          client_secret='33c03b29679c45ada65a04e65f8b98f2')
    crawler_process.start()
