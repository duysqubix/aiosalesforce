import asyncio
import logging
import os

from tempfile import NamedTemporaryFile
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
)

from aiosalesforce.utils import json_dumps

from .defs import (
    ColumnDelimiter,
    JobQueryInfo,
    JobQueryResult,
    LineEnding,
    OperationType,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .client import BulkClientV2


class BulkQueryClient:
    """
    Salesforce Bulk API 2.0 query client.

    This is a low-level client used to manage query jobs.

    Parameters
    ----------
    bulk_client : BulkClientV2
        Bulk client.
    """

    bulk_client: "BulkClientV2"
    base_url: str

    def __init__(self, bulk_client: "BulkClientV2") -> None:
        self.bulk_client = bulk_client
        self.base_url = f"{self.bulk_client.base_url}/query"

    async def create_job(
        self,
        query: str,
        line_ending: LineEnding,
        column_delimiter: ColumnDelimiter,
        operation: OperationType = "query",
    ) -> JobQueryInfo:
        if operation not in ("query", "queryAll"):
            raise ValueError(f"Invalid operation: {operation} for query job")

        payload: dict[str, str] = {
            "operation": operation,
            "query": query,
            "contentType": "CSV",
            "columnDelimiter": column_delimiter,
            "lineEnding": line_ending,
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
        }

        response = await self.bulk_client.salesforce_client.request(
            "POST",
            self.base_url,
            content=json_dumps(payload),
            headers=headers,
        )
        return JobQueryInfo.from_json(response.content)

    async def get_job(self, job_id: str) -> JobQueryInfo:
        response = await self.bulk_client.salesforce_client.request(
            "GET", f"{self.base_url}/{job_id}", headers={"Accept": "application/json"}
        )
        return JobQueryInfo.from_json(response.content)

    async def get_job_results_parallel(
        self, job_id: str
    ) -> AsyncIterator[JobQueryResult]:
        version = self.bulk_client.salesforce_client.version
        base_url = (
            f"{self.bulk_client.salesforce_client.base_url}/services/data/v{version}"
        )

        url = f"{base_url}/jobs/query/{job_id}/resultPages"
        while True:
            response = await self.bulk_client.salesforce_client.request(
                "GET",
                url,
            )

            data = response.json()
            uris = [d["resultLink"] for d in data["resultChunks"]]

            for uri in uris:
                url = base_url + uri
                response = await self.bulk_client.salesforce_client.request(
                    "GET",
                    url,
                    headers={"Accept": "application/json"},
                )
                yield JobQueryResult.from_response(response)

            if data["done"] is True:
                break
            # update url with next set of results
            url = data["nextRecordsUrl"]

    async def download(
        self,
        query: str,
        path: str,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 3,
    ) -> list[JobQueryResult]:
        if not os.path.exists(os.path.dirname(path)):
            raise FileNotFoundError(f"File {path} does not exist")

        job_info = await self.create_job(query, line_ending, column_delimiter)
        logger.info("Created job %s for %s", job_info.id, job_info.object)

        while True:
            job_info = await self.get_job(job_info.id)
            if job_info.state == "JobComplete":
                break
            await asyncio.sleep(wait)

        results = []

        num_records = 0
        async for job_result in self.get_job_results_parallel(job_info.id):
            num_records += job_result.number_of_records
            with NamedTemporaryFile(delete=False, mode="wb", dir=path) as f:
                f.write(job_result.results)
                results.append(
                    JobQueryResult(
                        number_of_records=job_result.number_of_records,
                        locator=job_result.locator,
                        path=f.name,
                        results=None,
                    )
                )

        logger.info(
            "Downloaded %s:%s records to %s",
            job_info.object,
            num_records,
            path,
        )
        return results
