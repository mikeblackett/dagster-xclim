from collections.abc import Sequence

import dagster as dg
import xclim.core as xcc
import xclim.core.calendar as xcal


class ResamplingPartitionsDefinition(dg.StaticPartitionsDefinition):
    """A static partitions definition whose partitions are resampling frequencies."""

    def __init__(self, partition_keys: Sequence[xcc.Freq]):
        try:
            for key in partition_keys:
                xcal.parse_offset(key)
        except ValueError as error:
            raise dg.DagsterInvalidDefinitionError(str(error)) from error

        super().__init__(partition_keys)
