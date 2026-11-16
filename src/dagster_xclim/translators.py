"""Translation of xclim indicators into dagster assets."""

import re
from collections.abc import Generator, Mapping
from typing import Final, cast

import dagster as dg
import xarray as xr
import xclim.core as xcc
import xclim.core.indicator as xci
from dagster_object_factory.layer import DependencySpec
from dagster_object_factory.translator import DagsterObjectTranslator

from dagster_xclim.helpers import format_metadata

VARIABLE_INPUT_KINDS: Final = {
    xcc.InputKind.VARIABLE,
    xcc.InputKind.OPTIONAL_VARIABLE,
}
DEFAULT_XCLIM_KINDS: Final = {"xarray"}
DEFAULT_KEYWORD_SEPARATOR: Final = re.compile(r"\s+")
DEFAULT_SRC_FREQ: Final = "D"


class DagsterXclimIndicatorTranslator[T: xci.Indicator](
    DagsterObjectTranslator[T, xr.DataArray]
):
    """A translator mapping xclim indicators to dagster assets.

    The indicator's identifier becomes the asset name, its ``outputs``
    become the asset's outputs, and its variable parameters become
    dependencies resolved against the layer's upstream layers.
    """

    def get_name(self, obj: T, /) -> str:
        """Return the asset name from the indicator identifier.

        Args:
            obj: The indicator to translate.

        Returns:
            The indicator's identifier with any module prefix removed.
        """
        # If the indicator comes from an IndicatorCollection,
        # it has the name of the module prepended with a period delimiter.
        return cast(str, obj.identifier).rpartition(".")[2]

    def _get_outputs_by_var_name(self, obj: T) -> dict[str, xci.Output]:
        """Return a mapping of ``var_name`` to ``Output``."""
        result: dict[str, xci.Output] = {}
        for output in obj.outputs:
            if output.var_name is None:
                raise dg.DagsterInvalidDefinitionError(
                    f'indicator outputs must have a "var_name".'
                    f"Output {output!r} has {output.var_name=}"
                )
            result[output.var_name] = output
        return result

    def get_output_names(self, obj: T, /) -> tuple[str, ...]:
        """Return the output names of the indicator.

        Args:
            obj: The indicator to translate.

        Returns:
            The var_name of each output entry, or the asset name if the
            indicator declares none.
        """

        return tuple(self._get_outputs_by_var_name(obj))

    def get_description(self, obj: T, /) -> str | None:
        """Return the asset description of the indicator.

        Args:
            obj: The indicator to translate.

        Returns:
            The title with the abstract appended, or the abstract alone if
            the title is missing.
        """
        if obj.title is None:
            return obj.abstract
        description = obj.title
        if obj.abstract:
            description += f"\n\n{obj.abstract}"
        return description

    def get_descriptions_by_output_name(self, obj: T, /) -> Mapping[str, str | None]:
        """Return the description of each output of the indicator.

        Args:
            obj: The indicator to translate.

        Returns:
            The description of each output entry, otherwise the shared
            description broadcast to every output.
        """
        default = self.get_description(obj)
        return {
            var_name: default
            if obj.n_outs == 1
            else output.attrs.get("description", default)
            for var_name, output in self._get_outputs_by_var_name(obj).items()
        }

    def resolve_dependency_specs(self, obj: T, /) -> Generator[DependencySpec]:
        """Resolve the indicator's variable parameters as layer dependencies.

        Args:
            obj: The indicator to translate.

        Yields:
            A DependencySpec for each parameter of a variable input kind,
            named after the parameter and resolved against the layer's
            layer_deps. Optional variables without a matching layer dep are
            skipped.

        Raises:
            dagster.DagsterInvalidDefinitionError: If a required variable
                parameter resolves to an output of no matching layer dep.
        """
        parameters = cast(dict[str, xci.Parameter], obj.parameters)
        for name, parameter in parameters.items():
            if parameter.kind not in VARIABLE_INPUT_KINDS:
                continue
            canonical_name = _resolve_canonical_parameter_name(parameter)
            layer_dep = self.layer.resolve_layer_dep(canonical_name)
            if layer_dep is None:
                if parameter.kind is xcc.InputKind.OPTIONAL_VARIABLE:
                    continue
                raise dg.DagsterInvalidDefinitionError(
                    f"{obj.identifier}: parameter {name!r} resolves to unknown "
                    f"variable {canonical_name!r}"
                )
            yield layer_dep.resolve_dependency(
                output_name=canonical_name, input_name=name
            )

    def get_metadata(self, obj: T, /) -> Mapping[str, dg.MetadataValue]:
        """Return the metadata of the indicator.

        Args:
            obj: The indicator to translate.

        Returns:
            Entries for the compute function (``compute``), the indicator
            class (``indicator_class``), and the source frequency
            (``src_freq``), plus the indicator's injected parameters
            (``injected_parameters``) if it has any.
        """
        metadata: dict[str, dg.MetadataValue] = {
            "compute": format_metadata(obj.compute),
            "indicator_class": format_metadata(obj.__class__),
            "src_freq": format_metadata(getattr(obj, "src_freq", DEFAULT_SRC_FREQ)),
        }
        if obj.injected_parameters:
            metadata["injected_parameters"] = format_metadata(obj.injected_parameters)
        return metadata

    def get_tags(self, obj: T, /) -> Mapping[str, str]:
        """Return the tags of the indicator.

        Args:
            obj: The indicator to translate.

        Returns:
            One tag per keyword, each set to "true".
        """
        return {k: "true" for k in obj.keywords if k}

    def get_kinds(self, obj: T, /) -> set[str] | None:
        """Return the kinds of the indicator.

        Args:
            obj: The indicator to translate.

        Returns:
            The default kinds for xclim indicator assets
            (``DEFAULT_XCLIM_KINDS``).
        """
        return DEFAULT_XCLIM_KINDS


def _resolve_canonical_parameter_name(parameter: xci.Parameter) -> str:
    """Resolve the canonical variable name of a parameter.

    Args:
        parameter: The indicator parameter.

    Returns:
        The parameter's string default if it has one, otherwise its compute
        name.
    """
    # handles parameter name overrides in virtual modules
    if isinstance(parameter.default, str):
        return parameter.default
    return parameter.compute_name
