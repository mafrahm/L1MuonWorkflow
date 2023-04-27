# coding: utf-8

"""
Exemplary selection methods for direct reduction
"""

from collections import defaultdict

from columnflow.columnar_util import set_ak_column, has_ak_column
from columnflow.util import maybe_import
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.production.util import attach_coffea_behavior
from columnflow.production.processes import process_ids
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.cms.seeds import deterministic_seeds

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(uses={
    attach_coffea_behavior, "L1Mu.pt", "L1Mu.phi", "L1Mu.eta", "L1Mu.mass",
})
def attach_coffea_behavior_l1(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    if not has_ak_column(events, "L1Mu.mass"):
        events = set_ak_column(events, "L1Mu.mass", 0.106)  # muon mass

    l1_collections = {
        "L1Mu": {
            "type_name": "Muon",
            "check_attr": "metric_table",
            "skip_fields": "*Idx*G",
        },
    }
    events = self[attach_coffea_behavior](
        events, collections=l1_collections, **kwargs,
    )

    return events


@selector(
    uses={"mc_weight"},
)
def cutflow_routine(self, events, stats, step, **kwargs):
    print(f"Number of events after step {step}: {len(events)}")
    stats[f"n_events_{step}"] += len(events)

    if self.dataset_inst.is_mc:
        stats[f"sum_mc_weight_{step}"] += ak.sum(events.mc_weight)


@selector(
    uses={
        cutflow_routine, attach_coffea_behavior_l1,
        mc_weight, process_ids, deterministic_seeds,
        "nMuon", "Muon.pt", "Muon.eta", "Muon.phi", "Muon.mass",
        "Muon.tightId", "Muon.mediumId", "Muon.charge",
        "L1Mu.pt", "L1Mu.eta", "L1Mu.phi", "L1Mu.mass", "L1Mu.bx", "L1Mu.hwQual",
    },
    produces={
        mc_weight, process_ids, deterministic_seeds,
        "ProbeMuon.*", "N_probes",
    },
    exposed=True,
)
def muon_reduction(
        self: Selector,
        events: ak.Array,
        stats: defaultdict,
        **kwargs,
) -> [ak.Array, SelectionResult]:

    stats["n_events"] += len(events)

    results = SelectionResult()

    # create process ids (used for normalization)
    events = self[process_ids](events, **kwargs)

    # deterministic seeds (needed?)
    # events = self[deterministic_seeds](events, **kwargs)

    # coffea behavior for L1 objects
    events = self[attach_coffea_behavior_l1](events, **kwargs)

    # add the mc weight
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    # Baseline Muon requirements
    muon_mask = (
        (events.Muon.pt > 3) &
        (abs(events.Muon.eta) < 2.5) &
        (events.Muon.mediumId)
    )
    set_ak_column(events, "Muon", events.Muon[muon_mask])

    # Require at least two muons
    muon_sel = ak.sum(muon_mask, axis=1) >= 2
    events = events[muon_sel]
    self[cutflow_routine](events, stats, "muon_pair")

    # Baseline TagMuon requirements (and require at least one)
    tag_reqs = (
        # (events.Muon.iso < self.tag_iso) &
        (events.Muon.pt > self.tag_pt)
    )
    if self.req_hlt:
        # TODO: which columns to use?
        tag_reqs = tag_reqs & (
            events.Muon.hlt_isomu != 0 &  # name?
            events.Muon.hlt_isoDeltaR < 0.1  # name?
        )

    events = set_ak_column(events, "TagMuon", events.Muon[tag_reqs])
    events = events[ak.num(events.TagMuon, axis=1) >= 1]
    self[cutflow_routine](events, stats, "tag_reqs")

    # Baseline L1TagMuon requirements
    l1tag_reqs = (
        (events.L1Mu.hwQual >= 12) &
        (events.L1Mu.pt > self.tag_pt - 4.01)
    )
    events = set_ak_column(events, "L1TagMuon", events.L1Mu[l1tag_reqs])
    self[cutflow_routine](events, stats, "l1tag_reqs")

    # Require at least 1 TagMuon with dR match to a L1TagMuon
    tag_matched_mask = ak.any(events.TagMuon.metric_table(events.L1TagMuon) < self.max_dr, axis=2)

    events = set_ak_column(events, "TagMuon", events.TagMuon[tag_matched_mask])
    events = events[ak.num(events.TagMuon, axis=1) >= 1]
    self[cutflow_routine](events, stats, "l1tag_match")

    # to simplify for now: only leading TagMuon
    # events = set_ak_column(events, "TagMuon", ak.from_regular(events.TagMuon[:, [0]]))

    # Baseline ProbeMuon requirements
    probe_reqs = (
        (events.Muon.pt > self.prb_pt)  # can I move this requirement to the producer?
    )
    events = set_ak_column(events, "ProbeMuon", events.Muon[probe_reqs])

    # broadcast arrays to calculate invariant mass and delta R
    probe, tag = ak.unzip(ak.cartesian([events.ProbeMuon, events.TagMuon], nested=True))
    tag = set_ak_column(tag, "dr", probe.delta_r(tag))
    tag = set_ak_column(tag, "m_inv", (tag + probe).mass)

    match_reqs = tag.dr > 2 * self.max_dr
    if self.req_z:
        match_reqs = match_reqs & (tag.m_inv > 81) & (tag.m_inv < 101)

    # store matched Tags as part of the ProbeMuons
    events["ProbeMuon", "TagMuon"] = tag[match_reqs]

    # probes with at least one match are kept
    events = set_ak_column(
        events, "ProbeMuon",
        events.ProbeMuon[ak.num(events.ProbeMuon.TagMuon, axis=2) >= 1],
    )

    # require at least one probe with m_inv match
    events = events[ak.num(events.ProbeMuon, axis=1) >= 1]
    self[cutflow_routine](events, stats, "probe_match")

    self[cutflow_routine](events, stats, "selected")

    # TODO: match L1Mu to Probe (without cutting), flatten ProbeMuons (+ required columns broadcasted)
    # and return flattened muons instead of events

    # match L1Mu to Probe
    probe, l1mu = ak.unzip(ak.cartesian([events.ProbeMuon, events.L1Mu], nested=True))
    l1mu = set_ak_column(l1mu, "dr", probe.delta_r(l1mu))
    events["ProbeMuon", "L1ProbeMuon"] = l1mu[l1mu.dr < self.max_dr]

    # sums per process id (for normalization weights)
    if self.dataset_inst.is_mc:
        stats.setdefault("sum_mc_weight_per_process", defaultdict(float))
        unique_process_ids = np.unique(events.process_id)
        for p in unique_process_ids:
            stats["sum_mc_weight_per_process"][int(p)] += ak.sum(
                events.mc_weight[events.process_id == p],
            )

    # store the number of probe muons (NOTE: changes if we define cuts on probes later)
    events = set_ak_column(events, "N_probes", ak.num(events.ProbeMuon, axis=1))

    # flatten ProbeMuon collection + some (broadcasted) other columns
    keep_columns = {"process_id", "event", "ProbeMuon", "N_probes"}
    if self.dataset_inst.is_mc:
        keep_columns.add("mc_weight")

    arrays = ak.flatten(ak.cartesian({field: events[field] for field in keep_columns}))

    return arrays, results


@muon_reduction.init
def muon_reduction_init(self: Selector) -> None:
    # define default settings that can be overwritten
    defaults = {
        "max_dr": 0.4,
        "tag_iso": 0.1,
        "tag_pt": 26.,
        "prb_pt": 22.,  # skip, use categories?
        "req_z": True,
        "req_hlt": False,
        "req_uGMT": True,
        "req_BXi": 0,
    }

    # set defaults to class attributes if not present
    for variable, value in defaults.items():
        if getattr(self, variable, None) is None:
            setattr(self, variable, value)

    # NOTE: it might also be nice to have these settings in the config (or even in some output txt file to)
