from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from functools import singledispatch
from os import PathLike
from typing import Any

import dagster as dg
import pandas as pd
import upath as up


@singledispatch
def format_metadata(value: Any) -> dg.MetadataValue:
    return dg.MetadataValue.text(str(value))


@format_metadata.register
def _(value: str) -> dg.TextMetadataValue | dg.UrlMetadataValue:
    if up.UPath(value).protocol in {"http", "https"}:
        return dg.MetadataValue.url(value)
    return dg.MetadataValue.text(value)


@format_metadata.register
def _(value: PathLike) -> dg.PathMetadataValue:
    return dg.MetadataValue.path(value)


@format_metadata.register
def _(value: None) -> dg.NullMetadataValue:
    return dg.MetadataValue.null()


@format_metadata.register
def _(value: int) -> dg.IntMetadataValue:
    return dg.MetadataValue.int(value)


@format_metadata.register
def _(value: float) -> dg.FloatMetadataValue:
    return dg.MetadataValue.float(value)


@format_metadata.register
def _(value: bool) -> dg.BoolMetadataValue:
    return dg.MetadataValue.bool(value)


@format_metadata.register
def _(value: Sequence | Mapping) -> dg.JsonMetadataValue:
    return dg.MetadataValue.json(value)


@format_metadata.register
def _(value: Callable) -> dg.PythonArtifactMetadataValue:
    return dg.MetadataValue.python_artifact(value)


@format_metadata.register
def _(value: datetime | pd.Timestamp) -> dg.TimestampMetadataValue:
    return dg.MetadataValue.timestamp(value)


@format_metadata.register
def _(value: dg.AssetKey) -> dg.DagsterAssetMetadataValue:
    return dg.MetadataValue.asset(value)
