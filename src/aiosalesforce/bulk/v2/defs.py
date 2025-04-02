import dataclasses
import datetime

from enum import Enum
from typing import Literal, Self, TypeAlias

from aiosalesforce.utils import json_loads

OperationType: TypeAlias = Literal[
    "insert",
    "delete",
    "hardDelete",
    "update",
    "upsert",
]


class ColumnDelimiter(str, Enum):
    BACKQUOTE = "BACKQUOTE"
    CARET = "CARET"
    COMMA = "COMMA"
    PIPE = "PIPE"
    SEMICOLON = "SEMICOLON"
    TAB = "TAB"


class LineEnding(str, Enum):
    LF = "LF"
    CRLF = "CRLF"


@dataclasses.dataclass
class JobIngestInfo:
    """Bulk API 2.0 ingest job information."""

    id: str
    operation: str
    object: str
    created_by_id: str
    created_date: datetime.datetime
    system_modstamp: datetime.datetime
    state: Literal[
        "Open",
        "UploadComplete",
        "InProgress",
        "JobComplete",
        "Aborted",
        "Failed",
    ]
    external_id_field_name: str | None
    concurrency_mode: Literal["Parallel"]
    content_type: Literal["CSV"]
    api_version: str
    job_type: Literal["V2Ingest"] | None
    content_url: str
    line_ending: Literal["LF", "CRLF"]
    column_delimiter: Literal[
        "BACKQUOTE",
        "CARET",
        "COMMA",
        "PIPE",
        "SEMICOLON",
        "TAB",
    ]

    @classmethod
    def from_json(cls, data: bytes) -> Self:
        job_info = cls(
            **{
                field.name: (_ := json_loads(data)).get(
                    "".join(
                        [
                            component.capitalize() if i > 0 else component
                            for i, component in enumerate(field.name.split("_"))
                        ]
                    ),
                    None,
                )
                for field in dataclasses.fields(cls)
            }
        )
        for attr in ["created_date", "system_modstamp"]:
            setattr(
                job_info,
                attr,
                datetime.datetime.fromisoformat(getattr(job_info, attr)),
            )
        return job_info


@dataclasses.dataclass
class JobIngestResult:
    """Bulk API 2.0 ingest job result."""

    job_info: JobIngestInfo
    successful_results: list[dict[str, str]]
    failed_results: list[dict[str, str]]
    unprocessed_records: list[dict[str, str]]
