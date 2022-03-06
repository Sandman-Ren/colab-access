import abc
import time
import urllib
from typing import Dict, Union, List, Tuple, Any
from urllib import request, parse

import os
import sys
from pathlib import Path
from os import PathLike

import base64

import json

import lyricsgenius
import lyricsgenius.types
import scrapy
import scrapy.crawler

import unicodedata

import multiprocessing

SPOTIFY_API_BASE_URL = r'https://api.spotify.com/v1'
SPOTIFY_OAUTH2_URL = r'https://accounts.spotify.com/api/token'


class JWTToken:
    pass


class ClientCredentialsJWTToken(JWTToken):

    @abc.abstractmethod
    def __init__(self, client_id: str = None, client_secret: str = None):
        self.client_id = client_id
        self.client_secret = client_secret

    @staticmethod
    def from_json_file(file: Path) -> "ClientCredentialsJWTToken":
        attributes: Dict = json.load(file.open('r'))
        token: ClientCredentialsJWTToken
        try:
            client_id = attributes['client_id']
            client_secret = attributes['client_secret']
            token = ClientCredentialsJWTToken(client_id, client_secret)
        except KeyError:
            raise ValueError("Expect field client_id and client_secret to be both present in the file.")
        return token

    @abc.abstractmethod
    def get_authorized_header(self) -> Dict[str, str]:
        pass


class SpotifyClientCredentialsJWTToken(ClientCredentialsJWTToken):
    AUTH_BODY = parse.urlencode([('grant_type', 'client_credentials')]).encode()
    SPOTIFY_SEARCH_ENDPOINT = r'/search'

    @staticmethod
    def from_json_file(file: Path) -> "SpotifyClientCredentialsJWTToken":
        attributes: Dict = json.load(file.open('r'))
        token: SpotifyClientCredentialsJWTToken
        try:
            client_id = attributes['client_id']
            client_secret = attributes['client_secret']
            token = SpotifyClientCredentialsJWTToken(client_id, client_secret)
        except KeyError:
            raise ValueError("Expect field client_id and client_secret to be both present in the file.")
        return token

    def __init__(self, client_id: str = None, client_secret: str = None, lazy_initialize=True):

        if client_id is None:
            try:
                client_id = os.environ['SPOTIFY_CLIENT_ID']
            except KeyError:
                raise Exception("You must specify client_id as an argument or set the CLIENT_ID environment variable.")

            try:
                client_secret = os.environ['SPOTIFY_CLIENT_SECRET']
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
        req = request.Request(SPOTIFY_OAUTH2_URL, data=SpotifyClientCredentialsJWTToken.AUTH_BODY, headers=auth_header,
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


class GeniusClientCredentialsJWTToken(ClientCredentialsJWTToken):

    @staticmethod
    def from_json_file(file: Path) -> "GeniusClientCredentialsJWTToken":
        attributes = json.load(file.open('r'))
        try:
            token = attributes['token']
        except KeyError:
            raise ValueError("Expect field token in to be present in the file.")
        return GeniusClientCredentialsJWTToken(token=token)

    def __init__(self, client_id: str = None, client_secret: str = None, token: str = None):
        if token is None:
            try:
                token = os.environ['GENIUS_TOKEN']
            except KeyError:
                raise ValueError(
                    "You must set genius api token as an argument or as an environment variable GENIUS_TOKEN")
        self.token = token

    def get_authorized_header(self) -> Dict[str, str]:
        return {'Authorization': f'Bearer {self.token}'}


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
        self.token = SpotifyClientCredentialsJWTToken(client_id, client_secret)

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


class LyricsProvider(abc.ABC):
    @abc.abstractmethod
    def get_lyrics(self):
        pass


class GeniusLyricsProvider(LyricsProvider):

    LYRICS_NOT_FOUND = '<LYRICS NOT FOUND>'

    def __init__(self, token: str = None, token_file: Path = None):

        self.token: GeniusClientCredentialsJWTToken
        self.lyrics_provider: lyricsgenius.Genius
        if token is None:
            if token_file is None:
                raise ValueError("You must specify either a token or a JSON file that contains the Genius API token.")
            else:
                self.token = GeniusClientCredentialsJWTToken.from_json_file(token_file)
        else:
            self.token = GeniusClientCredentialsJWTToken(token=token)
        self.lyrics_provider = lyricsgenius.Genius(self.token.token)
        # set up the lyrics provider
        self.lyrics_provider.verbose = False
        self.lyrics_provider.remove_section_headers = True
        self.lyrics_provider.retries = 3

    def _search_by_song_name_artist_name(self, song_name: str, artist_name: str) -> str:
        song: lyricsgenius.types.Song = self.lyrics_provider.search_song(song_name, artist=artist_name)
        if song is None:
            print(f'Lyrics for song: {song_name} by {artist_name} is not found!')
            return GeniusLyricsProvider.LYRICS_NOT_FOUND
        if song.title != song_name or song.artist != artist_name:
            print(
                f'Expected song name: {song_name}, actual: {song.title}\tExpected artist name: {artist_name}, actual: {song.artist}')
        lyrics = song.lyrics
        return lyrics

    def get_lyrics(self, song_name: str, artist_name: str):
        return self._search_by_song_name_artist_name(song_name, artist_name)


class SpotifySearchCrawlerWithLyrics(scrapy.Spider):
    name: str = "SpotifySearchCrawlerWithLyrics"

    def __init__(self, spotify_client_id: str, spotify_client_secret: str, genius_api_token: str, **kwargs):
        super().__init__(**kwargs)
        if spotify_client_id is None:
            try:
                spotify_client_id = os.environ['CLIENT_ID']
            except KeyError:
                raise Exception("You must specify client_id as an argument or set the CLIENT_ID environment variable.")

            try:
                spotify_client_secret = os.environ['CLIENT_SECRET']
            except KeyError:
                raise Exception(
                    "You must specify client_secret as an agrument or set the CLIENT_SECRET environment variable.")

        self.client_id = spotify_client_id
        self.client_secret = spotify_client_secret
        self.spotify_token = SpotifyClientCredentialsJWTToken(spotify_client_id, spotify_client_secret)
        self.genius_token = GeniusClientCredentialsJWTToken(token=genius_api_token)
        self.lyrics_provider = GeniusLyricsProvider(self.genius_token.token)

        self.output_dir = Path.cwd() / f'crawler-{self.name}-with_lyrics-output'
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

    def _populate_track_with_lyrics(self, response: scrapy.http.TextResponse, track: Dict[str, Any]):
        track['lyrics'] = response.json()['lyrics']

    def parse(self, response: scrapy.http.TextResponse, **kwargs):
        searched_tracks: dict = response.json()['tracks']
        # search lyrics in the item pipeline
        yield searched_tracks
        if 'next' in searched_tracks:
            yield self.get_authorized_request(url=searched_tracks['next'], callback=self.parse)

    def get_authorized_request(self, url, callback=None) -> scrapy.Request:

        authorized_header = self.spotify_token.get_authorized_header()
        authorized_header['Content-Type'] = 'application/json'
        return scrapy.Request(url=url, headers=authorized_header, callback=callback)


# class SearchedTrackLyricsItemPipeline:
#     def __init__(self):
#         self.spider = None
#         self.logger = None
#
#     def open_spider(self, spider: scrapy.Spider):
#         self.spider = spider
#         self.logger = spider.logger
#
#     def process_item(self, item: Dict[str, Union[str, Any]], spider: scrapy.Spider):
#         #

class SearchedTrackItemPipeline:
    """
    Pipeline for processing items scraped from the SpotifySearchCrawler.
    """

    def __init__(self):
        self.output_dir = None
        self.logger = None

    def _populate_song_lyrics(self, track: Dict[str, Any], lyrics_provider: GeniusLyricsProvider):
        first_artist = track['artists'][0]['name']
        song_name = track['name']
        self.logger.info(f'Populating lyrics for song: {song_name} by artist: {first_artist}')
        lyrics = lyrics_provider.get_lyrics(song_name, first_artist)
        if lyrics == GeniusLyricsProvider.LYRICS_NOT_FOUND:
            self.logger.info(f'Lyrics for song: {song_name} by artist: {first_artist} is not found!')
        track['lyrics'] = lyrics_provider.get_lyrics(song_name, first_artist)

    def open_spider(self, spider: scrapy.Spider):
        # set the output directory based on the information
        self.output_dir: Path = getattr(spider, 'output_dir')
        self.logger = spider.logger
        self.logger.info(f'SearchedTrackItemPipeline output directory: {self.output_dir}')

    def process_item(self, item: Dict[str, Union[str, Any]], spider: SpotifySearchCrawlerWithLyrics) -> Dict[str, Any]:
        self.logger.info(f'Processing item: {item["href"]}')
        # collect some metadata to store these objects
        href: str = item['href']
        parsed_href = parse.urlparse(href)
        query_parameters = parsed_href.query  # we will use this as the filename

        tracks: List[Dict[str, Any]] = item['items']
        for t in tracks:
            self._populate_song_lyrics(t, spider.lyrics_provider)

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
    crawler_process.crawl(SpotifySearchCrawlerWithLyrics, spotify_client_id='41d11c64c99f4295b22b262d02a041ff',
                          spotify_client_secret='33c03b29679c45ada65a04e65f8b98f2', genius_api_token='_0gkLXiRroGqnGmT2B-Sb6iVUxDxNqMGQtx3K5GSRCzU0AxcpY6orgYsHkE5pkZc')
    crawler_process.start()

    # load
