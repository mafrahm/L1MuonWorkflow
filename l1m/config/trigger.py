# coding: utf-8

"""
Definition of triggers
"""

import order as od
from columnflow.util import DotDict


def add_triggers(config: od.Config) -> None:

    trig_qual = {  # L1T Quality requirement for probe muon
        "SingleMu": [12, 13, 14, 15],
        "SingleMu7": [11, 12, 13, 14, 15],
        "DoubleMu": [8, 9, 10, 11, 12, 13, 14, 15],
        "MuOpen": [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        "TFMatch": [0],
    }
    trig_pt = {  # Minimum L1T pT for probe muon
        "SingleMu": 22,
        "SingleMu7": 7,
        "DoubleMu": 8,
        "MuOpen": 3,
        "TFMatch": 0,
    }
    trig_bit = {  # noqa
        "SingleMu": 16,
        "SingleMu7": 8,
        "DoubleMu": 4,
        "MuOpen": 2,
        "TFMatch": 1,
    }

    config.x.triggers = DotDict({})
    for trig_key in trig_qual.keys():
        config.x.triggers[trig_key] = DotDict({
            "name": trig_key,
            "pt": trig_pt[trig_key],
            "qual": trig_qual[trig_key],
        })
