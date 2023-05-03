# coding: utf-8

"""
CMS datasets from the 2023 data-taking campaign
"""

import cmsdb.processes as procs
from l1m.config.datasets.run3_2023_nano_v11p9_v1 import campaign_run3_2023_nano_v11p9_v1 as cpn

#
# Muons
#

cpn.add_dataset(
    name="prompt_data_mu0",
    id=14668024,
    is_data=True,
    processes=[procs.data_mu],
    keys=[
        "/Muon0/Run2023B-PromptNanoAODv11p9_v1-v1/NANOAOD",
    ],
    n_files=4,
    n_events=22623 + 210768 + 18187 + 35601,
    aux={
        "era": "A",
    },
)

cpn.add_dataset(
    name="prompt_data_mu1",
    id=14667991,
    is_data=True,
    processes=[procs.data_mu],
    keys=[
        "/Muon1/Run2023B-PromptNanoAODv11p9_v1-v1/NANOAOD"
    ],
    n_files=4,
    n_events=18273 + 10134 + 14865 + 36235,
    aux={
        "era": "A",
    },
)
