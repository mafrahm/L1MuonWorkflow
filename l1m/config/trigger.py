# coding: utf-8

"""
Definition of triggers
"""

import order as od
from columnflow.util import DotDict, maybe_import
from columnflow.selection import selector, Selector

ak = maybe_import("awkward")


trigger_names = ["SingleMu", "SingleMu7", "DoubleMu", "MuOpen", "TFMatch"]


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
    trig_id = {  # id to be added to category id
        "SingleMu": 1,
        "SingleMu7": 2,
        "DoubleMu": 3,
        "MuOpen": 4,
        "TFMatch": 5,
    }

    config.x.triggers = DotDict({})
    for trig_key in trig_qual.keys():
        config.x.triggers[trig_key] = DotDict({
            "name": trig_key,
            "id": trig_id[trig_key],
            "pt": trig_pt[trig_key],
            "qual": trig_qual[trig_key],
        })


def add_trigger_categories(config: od.Config) -> None:
    """
    Function that adds trigger categories to config.
    The 'all_probes' category will be used on the command line to create 1 plot
    containing efficiencies of all specified triggers. (TODO: efficiency plot function)
    """
    add_triggers(config)

    cat = config.add_category(
        name="all_probes",
        id=100,
        selection="incl",
        label="All probes",
    )
    cat.add_category(
        name="valid_probe",
        id=200,
        selection="valid_probe",
        label="Valid probe",
    )
    for trigger in config.x.triggers.values():
        cat.add_category(
            name=f"trig_{trigger.name}",
            id=200 + trigger.id,
            selection=f"{trigger.name}_sel",
            label=r"%s ($p_{T} > %.1f$, $qual \geq %i$)" % (trigger.name, trigger.pt, trigger.qual[0]),
            # label=f"{trigger.name} ($p_{T} > {trigger.pt}$, $qual >= {trigger.qual[0]}$)",
        )


@selector(uses={"ProbeMuon.TagMuon"})
def valid_probe(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.num(events.ProbeMuon.TagMuon, axis=1) >= 1


for trigger_name in trigger_names:
    @selector(
        uses={
            "ProbeMuon.TagMuon", "ProbeMuon.L1ProbeMuon.dr",
            "ProbeMuon.L1ProbeMuon.pt", "ProbeMuon.L1ProbeMuon.hwQual",
        },
        cls_name=f"{trigger_name}_sel",
    )
    def trigger_sel(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
        l1mu = events.ProbeMuon.L1ProbeMuon
        trigger = self.config_inst.x.triggers[self.cls_name.replace("_sel", "")]

        l1mu_reqs = (
            (l1mu.pt > trigger.pt) &
            (l1mu.hwQual >= trigger.qual[0]) &
            (l1mu.dr < 0.4)  # this is the dr to the ProbeMuon (should be required somewhere before)
        )

        mask = ak.num(l1mu[l1mu_reqs], axis=1) >= 1

        return mask
