import os
from pathlib import Path
from dagster import Definitions
from dagster.components import build_component_defs

components_dir = Path(__file__).parent / "components"
defs = build_component_defs(components_dir)
