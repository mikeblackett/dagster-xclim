from dagster_xclim.components import (
    XclimIndicatorFactory,
    XclimResamplingIndicatorFactory,
)
from dagster_xclim.partitions import (
    ResamplingPartitionsDefinition,
)
from dagster_xclim.translators import (
    DagsterXclimIndicatorTranslator,
)

xc.set_options(as_dataset=False)

__all__ = [
    "DagsterXclimIndicatorTranslator",
    "ResamplingPartitionsDefinition",
    "XclimIndicatorFactory",
    "XclimResamplingIndicatorFactory",
]
