from __future__ import annotations

import csv
import json
import re
import urllib.parse
import pkgutil
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account


ANDROID_PUBLISHER_SCOPES = ["https://www.googleapis.com/auth/androidpublisher"]
DEVSTORAGE_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"


def _read_schema(filename: str) -> Mapping[str, Any]:
    raw = pkgutil.get_data("source_google_play_console", f"schemas/{filename}")
    if raw is None:
        raise FileNotFoundError(f"Schema non trovato: {filename}")
    return json.loads(raw.decode("utf-8"))


def _parse_rfc3339_to_epoch_seconds(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _extract_review_last_modified_seconds(review: Mapping[str, Any]) -> Optional[int]:
    comments = review.get("comments") or []
    max_seconds: Optional[int] = None
    for comment in comments:
        user_comment = (comment or {}).get("userComment") or {}
        last_modified = user_comment.get("lastModified") or {}
        seconds = last_modified.get("seconds")
        if seconds is None:
            continue
        seconds_int = int(seconds)
        if max_seconds is None or seconds_int > max_seconds:
            max_seconds = seconds_int
    return max_seconds


@dataclass
class ServiceAccountTokenProvider:
    service_account_info_json: str
    scopes: List[str]

    def __post_init__(self) -> None:
        info = json.loads(self.service_account_info_json)
        self._credentials = service_account.Credentials.from_service_account_info(info, scopes=self.scopes)
        self._request = GoogleAuthRequest()

    def get_token(self) -> str:
        if not self._credentials.valid or self._credentials.expired or not self._credentials.token:
            self._credentials.refresh(self._request)
        return str(self._credentials.token)


class GooglePlayConsoleStream(HttpStream):
    url_base = "https://androidpublisher.googleapis.com/androidpublisher/v3/"
    schema_filename: str

    def __init__(self, token_provider: ServiceAccountTokenProvider, package_name: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._token_provider = token_provider
        self._package_name = package_name

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token_provider.get_token()}",
        }

    def get_json_schema(self) -> Mapping[str, Any]:
        return _read_schema(self.schema_filename)


class ReviewsStream(GooglePlayConsoleStream):
    name = "reviews"
    primary_key = "reviewId"
    cursor_field = "last_modified_seconds"
    schema_filename = "reviews.json"

    def __init__(
        self,
        token_provider: ServiceAccountTokenProvider,
        package_name: str,
        start_date: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(token_provider=token_provider, package_name=package_name, **kwargs)
        self._start_date_seconds = _parse_rfc3339_to_epoch_seconds(start_date) if start_date else None
        self._cursor_value: Optional[int] = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value is None:
            return {}
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        cursor_val = value.get(self.cursor_field)
        self._cursor_value = int(cursor_val) if cursor_val is not None else None

    def path(self, **kwargs: Any) -> str:
        return f"applications/{self._package_name}/reviews"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {"maxResults": 100}
        if next_page_token:
            params.update(next_page_token)
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        body = response.json()
        token_pagination = body.get("tokenPagination") or {}
        next_token = token_pagination.get("nextPageToken")
        if not next_token:
            return None
        return {"token": next_token}

    def parse_response(self, response: requests.Response, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        body = response.json()
        for review in body.get("reviews") or []:
            last_modified_seconds = _extract_review_last_modified_seconds(review)
            record: MutableMapping[str, Any] = dict(review)
            if last_modified_seconds is not None:
                record[self.cursor_field] = last_modified_seconds

            if self._start_date_seconds is not None and last_modified_seconds is not None:
                if last_modified_seconds < self._start_date_seconds:
                    continue

            if self._cursor_value is not None and last_modified_seconds is not None:
                if last_modified_seconds < self._cursor_value:
                    continue

            if last_modified_seconds is not None:
                if self._cursor_value is None or last_modified_seconds > self._cursor_value:
                    self._cursor_value = last_modified_seconds

            yield record

    def get_updated_state(self, current_stream_state: Mapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_val = current_stream_state.get(self.cursor_field)
        current_seconds = int(current_val) if current_val is not None else None
        latest_seconds = latest_record.get(self.cursor_field)
        latest_seconds_int = int(latest_seconds) if latest_seconds is not None else None

        if current_seconds is None:
            return {self.cursor_field: latest_seconds_int} if latest_seconds_int is not None else {}
        if latest_seconds_int is None:
            return {self.cursor_field: current_seconds}
        return {self.cursor_field: max(current_seconds, latest_seconds_int)}


class InAppProductsStream(GooglePlayConsoleStream):
    name = "in_app_products"
    primary_key = "sku"
    schema_filename = "in_app_products.json"

    def path(self, **kwargs: Any) -> str:
        return f"applications/{self._package_name}/inappproducts"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {"maxResults": 100}
        if next_page_token:
            params.update(next_page_token)
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        body = response.json()
        token_pagination = body.get("tokenPagination") or {}
        next_token = token_pagination.get("nextPageToken")
        if not next_token:
            return None
        return {"token": next_token}

    def parse_response(self, response: requests.Response, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        body = response.json()
        yield from (body.get("inappproduct") or [])


class SubscriptionsStream(GooglePlayConsoleStream):
    name = "subscriptions"
    primary_key = "productId"
    schema_filename = "subscriptions.json"

    def path(self, **kwargs: Any) -> str:
        return f"applications/{self._package_name}/monetization/subscriptions"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {"pageSize": 100}
        if next_page_token:
            params.update(next_page_token)
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        body = response.json()
        next_token = body.get("nextPageToken")
        if not next_token:
            return None
        return {"pageToken": next_token}

    def parse_response(self, response: requests.Response, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        body = response.json()
        yield from (body.get("subscriptions") or [])


class StatsInstallsOverviewStream(Stream):
    name = "stats_installs"
    schema_filename = "stats_installs_overview.json"

    def __init__(
        self,
        token_provider: ServiceAccountTokenProvider,
        bucket_id: str,
        package_name: str,
        breakdown: str = "overview",
        month: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._token_provider = token_provider
        self._bucket_id = bucket_id
        self._package_name = package_name
        self._breakdown = breakdown
        self._month = month

    def get_json_schema(self) -> Mapping[str, Any]:
        return _read_schema(self.schema_filename)

    def _request_headers(self) -> Mapping[str, Any]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token_provider.get_token()}",
        }

    def _list_objects(self, prefix: str) -> Iterable[Mapping[str, Any]]:
        url = f"https://storage.googleapis.com/storage/v1/b/{self._bucket_id}/o"
        params: MutableMapping[str, Any] = {"prefix": prefix, "maxResults": 1000}
        page_token: Optional[str] = None
        while True:
            if page_token:
                params["pageToken"] = page_token
            resp = requests.get(url, headers=self._request_headers(), params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("items") or []:
                yield item
            page_token = payload.get("nextPageToken")
            if not page_token:
                return

    def _download_object_bytes(self, object_name: str) -> bytes:
        encoded_name = urllib.parse.quote(object_name, safe="")
        url = f"https://storage.googleapis.com/storage/v1/b/{self._bucket_id}/o/{encoded_name}"
        resp = requests.get(url, headers=self._request_headers(), params={"alt": "media"}, timeout=300)
        resp.raise_for_status()
        return resp.content

    def _decode_csv(self, raw: bytes) -> str:
        try:
            return raw.decode("utf-16")
        except UnicodeError:
            return raw.decode("utf-8-sig")

    def _matches(self, object_name: str) -> bool:
        if not object_name.lower().endswith(".csv"):
            return False
        if self._package_name not in object_name:
            return False
        if not object_name.endswith(f"{self._breakdown}.csv"):
            return False
        if self._month and self._month not in object_name:
            return False
        return True

    def read_records(
        self,
        sync_mode: Any,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        prefix = "stats/installs"
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"

        for item in self._list_objects(prefix=prefix):
            object_name = str(item.get("name") or "")
            if not self._matches(object_name):
                continue

            raw = self._download_object_bytes(object_name)
            text = self._decode_csv(raw)

            match = re.search(r"(\d{6})", object_name)
            report_month = match.group(1) if match else None

            reader = csv.DictReader(StringIO(text))
            for row in reader:
                record: MutableMapping[str, Any] = dict(row)
                record["report_bucket_id"] = self._bucket_id
                record["report_object"] = object_name
                record["report_month"] = report_month
                record["report_breakdown"] = self._breakdown
                updated = item.get("updated")
                if updated is not None:
                    record["report_object_updated_at"] = updated
                yield record
