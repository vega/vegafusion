# Ignore all mypy errors for now since traitlets aren't well typed
# type: ignore
from __future__ import annotations

import pathlib
from typing import Any

import anywidget
import traitlets

from vegafusion import runtime
from vegafusion.runtime import PreTransformWarning
from vegafusion.transformer import DataFrameLike

_here = pathlib.Path(__file__).parent


def load_js_src() -> str:
    return (_here / "js" / "index.js").read_text()


class VegaFusionWidget(anywidget.AnyWidget):
    _esm = load_js_src()
    _css = r"""
    .vega-embed {
        /* Make sure action menu isn't cut off */
        overflow: visible;
    }
    """

    # Public traitlets
    spec = traitlets.Dict(allow_none=True)
    transformed_spec = traitlets.Dict(allow_none=True).tag(sync=True)
    server_spec = traitlets.Dict(allow_none=True).tag(sync=False)
    client_spec = traitlets.Dict(allow_none=True).tag(sync=False)
    comm_plan = traitlets.Dict(allow_none=True).tag(sync=False)
    warnings = traitlets.Dict(allow_none=True).tag(sync=False)

    inline_datasets = traitlets.Dict(default_value=None, allow_none=True)
    debounce_wait = traitlets.Float(default_value=10).tag(sync=True)
    max_wait = traitlets.Bool(default_value=True).tag(sync=True)
    local_tz = traitlets.Unicode(default_value=None, allow_none=True).tag(sync=True)
    embed_options = traitlets.Dict(default_value=None, allow_none=True).tag(sync=True)
    debug = traitlets.Bool(default_value=False)
    row_limit = traitlets.Int(default_value=100000).tag(sync=True)

    # Public output traitlets
    warnings = traitlets.List(allow_none=True)

    # Internal comm traitlets for VegaFusion support
    _js_watch_plan = traitlets.Any(allow_none=True).tag(sync=True)
    _js_to_py_updates = traitlets.Any(allow_none=True).tag(sync=True)
    _py_to_js_updates = traitlets.Any(allow_none=True).tag(sync=True)

    # Other internal state
    _chart_state = traitlets.Any(allow_none=True)

    # Track whether widget is configured for offline use
    _is_offline = False

    @classmethod
    def enable_offline(cls, offline: bool = True) -> None:
        """Configure VegaFusionWidget's offline behavior.

        Args:
            offline: If True, configure VegaFusionWidget to operate in offline mode
                where JavaScript dependencies are loaded from vl-convert. If False,
                configure it to operate in online mode where JavaScript dependencies
                are loaded from CDN dynamically. This is the default behavior.
        """
        import vl_convert as vlc

        if offline:
            if cls._is_offline:
                # Already offline
                return

            src_lines = load_js_src().split("\n")

            # Remove leading lines with only whitespace, comments, or imports
            while src_lines and (
                len(src_lines[0].strip()) == 0
                or src_lines[0].startswith("import")
                or src_lines[0].startswith("//")
            ):
                src_lines.pop(0)

            src = "\n".join(src_lines)

            # vl-convert's javascript_bundle function creates a self-contained
            # JavaScript bundle for JavaScript snippets that import from a small
            # set of dependencies that vl-convert includes. To see the available
            # imports and their imported names, run
            #       import vl_convert as vlc
            #       help(vlc.javascript_bundle)
            bundled_src = vlc.javascript_bundle(src)
            cls._esm = bundled_src
            cls._is_offline = True
        else:
            cls._esm = load_js_src()
            cls._is_offline = False

    def __init__(
        self,
        spec: dict[str, Any],
        inline_datasets: dict[str, DataFrameLike] | None = None,
        debounce_wait: int = 10,
        max_wait: bool = True,
        debug: bool = False,
        embed_options: dict[str, Any] | None = None,
        local_tz: str | None = None,
        row_limit: int = 100000,
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Jupyter Widget for displaying Vega chart specifications, using VegaFusion
        for server-side scaling.

        Args:
            spec: Vega chart specification.
            inline_datasets: Datasets referenced in the Vega spec in
                vegafusion+dataset:// URLs.
            debounce_wait: Debouncing wait time in milliseconds. Updates will be
                sent from the client to the kernel after debounce_wait
                milliseconds of no chart interactions.
            max_wait: If True (default), updates will be sent from the client to
                the kernel every debounce_wait milliseconds even if there are
                ongoing chart interactions. If False, updates will not be sent
                until chart interactions have completed.
            debug: If True, debug messages will be printed.
            embed_options: Options to pass to vega-embed. See
                https://github.com/vega/vega-embed?tab=readme-ov-file#options
            local_tz: Timezone to use for the chart. If None, the chart will use
                the browser's local timezone.
            row_limit: Maximum number of rows to send to the browser, after
                VegaFusion has performed its transformations. A RowLimitError
                will be raised if the VegaFusion operation results in more than
                row_limit rows.
        """
        super().__init__(
            spec=spec,
            inline_datasets=inline_datasets,
            debounce_wait=debounce_wait,
            max_wait=max_wait,
            debug=debug,
            embed_options=embed_options,
            local_tz=local_tz,
            row_limit=row_limit,
            **kwargs,
        )
        self.on_msg(self._handle_custom_msg)

    @traitlets.observe("spec")
    def _on_change_spec(self, change: dict[str, Any]) -> None:
        """
        Internal callback function that updates the widgets's internal
        state when the Vega chart specification changes
        """
        new_spec = change["new"]

        if new_spec is None:
            # Clear state
            with self.hold_sync():
                self.transformed_spec = None
                self.server_spec = None
                self.client_spec = None
                self.comm_plan = None
                self.warnings = None
                self._chart_state = None
                self._js_watch_plan = None
            return

        if self.local_tz is None:

            def on_local_tz_change(change: dict[str, Any]) -> None:
                self._init_chart_state(change["new"])

            self.observe(on_local_tz_change, ["local_tz"])
        else:
            self._init_chart_state(self.local_tz)

    @traitlets.observe("inline_datasets")
    def _on_change_inline_datasets(self, change: dict[str, Any]) -> None:
        """
        Internal callback function that updates the widgets's internal
        state when the inline datasets change
        """
        self._init_chart_state(self.local_tz)

    def _handle_custom_msg(self, content: dict[str, Any], buffers: Any) -> None:  # noqa: ANN401
        if content.get("type") == "update_state":
            self._handle_update_state(content.get("updates", []))

    def _handle_update_state(self, updates: list[dict[str, Any]]) -> None:
        """
        Handle the 'update_state' message from JavaScript
        """
        if self.debug:
            print(f"Received update_state message from JavaScript:\n{updates}")

        # Process the updates using the chart state
        if self._chart_state is not None:
            processed_updates = self._chart_state.update(updates)

            if self.debug:
                print(f"Processed updates:\n{processed_updates}")

            # Send the processed updates back to JavaScript
            self.send({"type": "update_view", "updates": processed_updates})
        else:
            print(
                "Warning: Received update_state message, but chart state is not "
                "initialized."
            )

    def _init_chart_state(self, local_tz: str) -> None:
        if self.spec is not None:
            with self.hold_sync():
                # Build the chart state
                self._chart_state = runtime.new_chart_state(
                    self.spec,
                    local_tz=local_tz,
                    inline_datasets=self.inline_datasets,
                    row_limit=self.row_limit,
                )

                # Check if the row limit was exceeded
                handle_row_limit_exceeded(
                    self.row_limit, self._chart_state.get_warnings()
                )

                # Get the watch plan and transformed spec
                self._js_watch_plan = self._chart_state.get_comm_plan()[
                    "client_to_server"
                ]
                self.transformed_spec = self._chart_state.get_transformed_spec()
                self.server_spec = self._chart_state.get_server_spec()
                self.client_spec = self._chart_state.get_client_spec()
                self.comm_plan = self._chart_state.get_comm_plan()
                self.warnings = self._chart_state.get_warnings()


def handle_row_limit_exceeded(
    row_limit: int, warnings: list[PreTransformWarning]
) -> None:
    for warning in warnings:
        if warning["type"] == "RowLimitExceeded":
            msg = (
                "The number of dataset rows after filtering and aggregation\n"
                f"exceeds the current limit of {row_limit}. Try adding an\n"
                "aggregation to reduce the size of the dataset that must be\n"
                "loaded into the browser. Or, disable the limit by setting the\n"
                "row_limit traitlet to None. Note that disabling this limit may\n"
                "cause the browser to freeze or crash."
            )
            raise RowLimitExceededError(msg)


class RowLimitExceededError(Exception):
    """
    Exception raised when the number of dataset rows after filtering and aggregation
    exceeds the current limit.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
