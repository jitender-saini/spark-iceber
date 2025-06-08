import json
import time
from datetime import datetime
from http import HTTPStatus

import requests

from util.logging import get_logger

log = get_logger(__name__)


# https://developer.themoviedb.org/reference/


class TMDBApi:
    def __init__(self, api_key: str, temp_dir: str):
        self.api_key = api_key
        self.temp_dir = temp_dir
        self.base_url = 'https://api.themoviedb.org/3'
        self.headers = {'accept': 'application/json', 'Authorization': f'Bearer {self.api_key}'}

    def fetch_api_data(self, endpoint: str, max_pages: int, params, start_page: int = 1) -> list[dict]:
        all_result: list[dict] = []
        current_page = start_page
        params = self._prepare_params(params, start_page)
        log.info('Fetching data from TMDB API with params: %s', params)

        while True:
            try:
                data = self._make_api_request(endpoint, params)
                result = data.get('results', [])

                if not result:
                    log.info(f'No results found on page {current_page}. Stopping pagination.')
                    break

                all_result.extend(result)
                log.info(f'Fetched page {current_page} with {len(result)} movies out of page {data["total_pages"]}')

                if self._should_stop_pagination(current_page, max_pages, data['total_pages']):
                    break

                current_page += 1
                params['page'] = current_page
                time.sleep(0.25)  # Respect rate limits (40 requests per 10 seconds)

            except Exception as e:
                log.error(f'Error occurred: {e}')
                self._handle_error(e, all_result, endpoint)

        return all_result

    @staticmethod
    def _prepare_params(params: dict | None, page: int) -> dict:
        return {'page': page} if params is None else params.update({'page': page}) or params

    def _make_api_request(self, endpoint: str, params: dict) -> dict:
        response = requests.get(f'{self.base_url}/{endpoint}', headers=self.headers, params=params)

        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            return self._handle_rate_limit(endpoint, params, int(response.headers.get('Retry-After')))

        if response.status_code != HTTPStatus.OK:
            raise Exception(f'Error fetching data: {response.status_code} - {response.text}')

        return response.json()

    def _handle_rate_limit(self, endpoint: str, params: dict, retry_after: int = 10) -> dict:
        log.info(f'Rate limit hit. Waiting {retry_after} seconds...')
        time.sleep(retry_after)
        return self._make_api_request(endpoint, params)

    def _handle_error(self, e, results: list[dict], endpoint: str) -> None:
        log.error(f'Error occurred: {e}')
        self._save_temp_data(results, endpoint.replace('/', '_') + '_temp_data.parquet')
        raise Exception(f'Failed to fetch data from TMDB API: {e}')

    def _save_temp_data(self, data: list[dict], file_name: str) -> None:
        file_path = f'{self.temp_dir}/{file_name}'
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        log.info(f'Saved {len(data)} records to temp file: {file_path}')

    @staticmethod
    def _should_stop_pagination(current_page: int, max_pages: int, total_pages: int) -> bool:
        return (max_pages and current_page >= max_pages) or current_page >= total_pages

    def fetch_movies(self, bookmark: datetime, max_pages: int, start_page: int = 1) -> list[dict]:
        params = {'sort_by': 'primary_release_date', 'release_date.gte': str(bookmark.date())}
        movies = self.fetch_api_data('discover/movie', params=params, start_page=start_page, max_pages=max_pages)
        return movies
