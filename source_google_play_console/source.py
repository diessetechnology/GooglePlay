from __future__ import annotations

import json
import pkgutil
from typing import Any, List, Mapping, MutableMapping, Tuple

import requests
from airbyte_cdk.models import ConnectorSpecification
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import (
    ANDROID_PUBLISHER_SCOPES,
    DEVSTORAGE_READ_ONLY_SCOPE,
    InAppProductsStream,
    ReviewsStream,
    ServiceAccountTokenProvider,
    StatsInstallsOverviewStream,
    SubscriptionsStream,
)


class SourceGooglePlayConsole(AbstractSource):
    def spec(self, logger: Any) -> ConnectorSpecification:
        raw = pkgutil.get_data("source_google_play_console", "spec.json")
        if raw is None:
            raise FileNotFoundError("spec.json non trovato nel package source_google_play_console")
        return ConnectorSpecification.parse_obj(json.loads(raw.decode("utf-8")))

    def check_connection(self, logger: Any, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        package_name = str(config["package_name"])
        token_provider = ServiceAccountTokenProvider(
            service_account_info_json=str(config["service_account_info"]),
            scopes=ANDROID_PUBLISHER_SCOPES,
        )

        url = f"https://androidpublisher.googleapis.com/androidpublisher/v3/applications/{package_name}/reviews"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token_provider.get_token()}",
        }
        params: MutableMapping[str, Any] = {"maxResults": 1}

        resp = requests.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code >= 400:
            return False, f"HTTP {resp.status_code}: {resp.text}"

        reports_bucket_id = config.get("reports_bucket_id")
        if reports_bucket_id:
            gcs_token_provider = ServiceAccountTokenProvider(
                service_account_info_json=str(config["service_account_info"]),
                scopes=[DEVSTORAGE_READ_ONLY_SCOPE],
            )
            gcs_url = f"https://storage.googleapis.com/storage/v1/b/{reports_bucket_id}/o"
            gcs_params: MutableMapping[str, Any] = {"prefix": "stats/installs", "maxResults": 1}
            gcs_headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {gcs_token_provider.get_token()}",
            }
            gcs_resp = requests.get(gcs_url, headers=gcs_headers, params=gcs_params, timeout=60)
            if gcs_resp.status_code >= 400:
                return False, f"GCS HTTP {gcs_resp.status_code}: {gcs_resp.text}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token_provider = ServiceAccountTokenProvider(
            service_account_info_json=str(config["service_account_info"]),
            scopes=ANDROID_PUBLISHER_SCOPES,
        )
        package_name = str(config["package_name"])
        start_date = config.get("start_date")

        streams: List[Stream] = [
            ReviewsStream(token_provider=token_provider, package_name=package_name, start_date=str(start_date) if start_date else None),
            InAppProductsStream(token_provider=token_provider, package_name=package_name),
            SubscriptionsStream(token_provider=token_provider, package_name=package_name),
        ]

        reports_bucket_id = config.get("reports_bucket_id")
        if reports_bucket_id:
            gcs_token_provider = ServiceAccountTokenProvider(
                service_account_info_json=str(config["service_account_info"]),
                scopes=[DEVSTORAGE_READ_ONLY_SCOPE],
            )
            breakdown = str(config.get("stats_installs_breakdown") or "overview")
            month = config.get("stats_installs_month")
            streams.append(
                StatsInstallsOverviewStream(
                    token_provider=gcs_token_provider,
                    bucket_id=str(reports_bucket_id),
                    package_name=package_name,
                    breakdown=breakdown,
                    month=str(month) if month else None,
                )
            )

        return streams
