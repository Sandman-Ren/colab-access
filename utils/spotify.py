import abc
import base64
import json
import logging
import multiprocessing
import os
import time
from os import PathLike
from pathlib import Path
from typing import Dict, Union, List, Any
from urllib import request, parse

import scrapy
import scrapy.crawler

SPOTIFY_API_BASE_URL = r'https://api.spotify.com/v1'
SPOTIFY_OAUTH2_URL = r'https://accounts.spotify.com/api/token'
GENIUS_BASE_URL = r'https://genius.com'
GENIUS_API_SEARCH_ENDPOINT = r'https://api.genius.com/search'


class JWTToken:
    pass


class SimpleJWTToken(JWTToken):
    pass


class GeniusAPIJWTToken(JWTToken):

    @staticmethod
    def from_json_file(file_path: PathLike) -> "GeniusAPIJWTToken":
        path = file_path
        if not isinstance(file_path, Path):
            path = Path(file_path)
        data = json.load(path.open('r', encoding='utf-8'))

        if 'token' not in data:
            raise KeyError(f'The json object loaded from file {str(path)} does not contain a "token" attribute.')
        return GeniusAPIJWTToken(data['token'])

    def __init__(self, token: str):
        self.token = token

    def get_authorization_header(self) -> Dict[str, str]:
        return {
            'Authorization': f'Bearer {self.token}'
        }


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


class SpotifySearchCrawlerWithLyrics(scrapy.Spider):
    name: str = "SpotifySearchCrawlerWithLyrics"

    LYRICS_NOT_FOUND = '<LYRICS-NOT-FOUND>'

    def __init__(self, spotify_client_id: str, spotify_client_secret: str, genius_api_token: str, exact: bool = False,
                 **kwargs):
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
        self.genius_token = GeniusAPIJWTToken(token=genius_api_token)
        self.require_exact = exact
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

    def parse(self, response: scrapy.http.TextResponse, **kwargs):
        searched_tracks: dict = response.json()['tracks']
        # search lyrics for each track
        # collect some metadata to store these objects
        href: str = searched_tracks['href']
        parsed_href = parse.urlparse(href)
        query_parameters = parsed_href.query  # we will use this as the filename
        tracks: List[Dict[str, Any]] = searched_tracks['items']
        for track in tracks:
            # add the search query parameters to the track, we will use this to store each song
            track['pipeline_identifier'] = query_parameters
            first_artist = track['artists'][0]['name']
            song_name = track['name']
            self.logger.info(f'Searching lyrics for song: {song_name} by artist: {first_artist}')
            query_string = parse.urlencode([('q', f'{song_name} {first_artist}')])
            yield scrapy.Request(url=f'{GENIUS_API_SEARCH_ENDPOINT}?{query_string}',
                                 headers=self.genius_token.get_authorization_header(),
                                 callback=self.parse_lyrics_search_result, cb_kwargs={
                    "song_name": song_name,
                    "artist_name": first_artist,
                    "exact": self.require_exact,
                    "track": track
                })

        if 'next' in searched_tracks:
            # search for next page of results
            yield self.get_authorized_request(url=searched_tracks['next'], callback=self.parse)

    def parse_lyrics_search_result(self, response: scrapy.http.TextResponse, song_name: str, artist_name: str,
                                   exact: bool, track: Dict[str, Any]):
        data: Dict[str, Dict[str, Any]] = response.json()
        candidates: List[Dict[str, Any]] = data['response']['hits']

        artist_name_lower = artist_name.lower()
        song_name_lower = song_name.lower()
        for c in candidates:
            if c['type'] == 'song':
                song_data: Dict[str, Any] = c['result']
                if not exact or artist_name_lower in song_data['artist_names'].lower() and song_name_lower in song_data[
                    'title'].lower():
                    yield scrapy.Request(f'{GENIUS_BASE_URL}{song_data["path"]}',
                                         callback=self.populate_song_lyrics, cb_kwargs={
                            "track": track,
                            "lyrics_artist_name": song_data['artist_names'],
                            "lyrics_song_name": song_data['title']
                        })
        track['lyrics'] = SpotifySearchCrawlerWithLyrics.LYRICS_NOT_FOUND
        track['lyrics-song-name'] = SpotifySearchCrawlerWithLyrics.LYRICS_NOT_FOUND
        track['lyrics-artist-name'] = SpotifySearchCrawlerWithLyrics.LYRICS_NOT_FOUND
        self.logger.info(f"Lyrics not found for song: {song_name}\tartist: {artist_name}")

    def populate_song_lyrics(self, response: scrapy.http.HtmlResponse, lyrics_song_name: str, lyrics_artist_name: str,
                             track: Dict[str, Any]):
        lyrics_sections = response.css('div[data-lyrics-container="true"] a > span::text').getall()
        lyrics = ''.join(lyrics_sections)
        track['lyrics'] = lyrics
        track['lyrics-song-name'] = lyrics_song_name
        track['lyrics-artist-name'] = lyrics_artist_name
        yield track

    def get_authorized_request(self, url, callback=None) -> scrapy.Request:
        authorized_header = self.spotify_token.get_authorized_header()
        authorized_header['Content-Type'] = 'application/json'
        return scrapy.Request(url=url, headers=authorized_header, callback=callback)


class TrackItemPipeline:
    legalizer: Dict[str, str] = {
        '\"': '((dquote))',
        '*': '((asterisk))',
        '<': '((lt))',
        '>': '((gt))',
        '?': '((q))',
        '\\': '((bslash))',
        '/': '((fslash))',
        '|': '((pipe))',
        ':': '((colon))'
    }

    def __init__(self):
        self.logger: logging.Logger
        self.output_dir: Path

    def legalize_filename(self, filename: str) -> str:
        for invalid_char, valid_char in TrackItemPipeline.legalizer.items():
            filename = filename.replace(invalid_char, valid_char)
        return filename

    def open_spider(self, spider: SpotifySearchCrawlerWithLyrics):
        self.logger = spider.logger
        self.output_dir = spider.output_dir

    def process_item(self, item: Dict[str, Any], spider: SpotifySearchCrawlerWithLyrics):
        # item is a single spotify track
        identifier = item['pipeline_identifier']
        first_artist = item['artists'][0]['name']
        song_name = item['name']
        song_id = item['id']

        # write to file
        output_filename: Path = self.output_dir / Path(self.legalize_filename(f'{identifier}-{song_id}.json'))
        json.dump(item, output_filename.open(mode='w', encoding='utf-8'))
        self.logger.info(
            f"TrackItemPipeline: song: {song_name}\tartist: {first_artist} written to: {str(output_filename)}")
        return item


if __name__ == '__main__':
    s = SpotifySearchCrawler(client_id='41d11c64c99f4295b22b262d02a041ff',
                             client_secret='33c03b29679c45ada65a04e65f8b98f2')
    crawler_process = scrapy.crawler.CrawlerProcess(settings={
        'ITEM_PIPELINES': {
            'spotify.TrackItemPipeline': 800
        },
        'LOG_LEVEL': "INFO",
        # on fast machines with multiple cores we need some auto throttling otherwise we will be banned from Genius
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 0.01,
        'AUTOTHROTTLE_MAX_DELAY': 3.00,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': min(8, multiprocessing.cpu_count())
    })
    print(vars(crawler_process.settings))
    crawler_process.crawl(SpotifySearchCrawlerWithLyrics, spotify_client_id='41d11c64c99f4295b22b262d02a041ff',
                          spotify_client_secret='33c03b29679c45ada65a04e65f8b98f2',
                          genius_api_token='kmmbgInH9WZwPAZJDBK77GG23U7LzMQntci4SqLqU10WcuBdmmi-kB5SYHLbnvK7',
                          exact=False)
    crawler_process.start()

    # load
