# coding: utf-8
# flake8: noqa

from l1m.columnflow_patches import patch_all


__all__ = [
    "config_2017",
]

# provisioning imports
from l1m.config.analysis_l1m import config_2017


# apply cf patches once
patch_all()
