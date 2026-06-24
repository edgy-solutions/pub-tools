import logging
import os
from pathlib import Path

from dagster import Definitions
from dagster.components import build_component_defs

components_dir = Path(__file__).parent / "components"
defs = build_component_defs(components_dir)


# --------------------------------------------------------------------------
# iagent registry sidecar (canonical integration example)
# --------------------------------------------------------------------------
# pub-tools is the first real (non-dummy) example of a Dagster user-
# deployment that publishes its asset metadata to the iagent
# ``domain-broker`` registry. The sidecar reads pub-tools' own
# ``Definitions``, derives a URN per asset, and POSTs each one to the
# broker so iagent's data clients (CortexDataClient, central-gateway)
# can resolve those URNs to physical data paths.
#
# Pattern any other user-deployment can copy:
#   1. Add ``dag-tools`` to your project dependencies.
#   2. At the bottom of your ``definitions.py``, after ``defs`` is
#      built, call ``publish_to_registry_at_startup(defs, location=...)``.
#   3. Set ``MESH_REGISTRY_URL`` in the deployment's env to the broker
#      URL (e.g. ``http://iagent-domain-broker:8000``). Unset = no-op,
#      so local dev / tests stay quiet.
#
# The sidecar is non-fatal by design: any failure (broker unreachable,
# inventory extractor missing, network blip) is logged at WARNING and
# the code-location boots normally. A user-deployment that can't reach
# the registry today still produces its data; it just isn't queryable
# through iagent until the registry can see it.
try:
    from dag_tools.sidecar import publish_to_registry_at_startup
    publish_to_registry_at_startup(defs, location="pub-tools")
except Exception as _sidecar_exc:  # pragma: no cover — operational
    # Catchall so a sidecar bug never wedges code-location import.
    # The standalone ``publish_to_registry_at_startup`` already has
    # its own try/except for the publish loop; this outer guard
    # protects against import-time errors (e.g. an older dag-tools
    # without the sidecar submodule).
    logging.getLogger(__name__).warning(
        "iagent sidecar import failed (non-fatal): %s", _sidecar_exc,
    )
