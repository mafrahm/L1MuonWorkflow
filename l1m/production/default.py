# coding: utf-8

"""
Column production methods related to higher-level features.
"""


from columnflow.production import Producer, producer
from columnflow.production.categories import category_ids
from columnflow.production.normalization import normalization_weights
from columnflow.util import maybe_import


np = maybe_import("numpy")
ak = maybe_import("awkward")


custom_collections = {
    "TagMuon": {
        "type_name": "Muon",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    "ProbeMuon": {
        "type_name": "Muon",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    "L1Mu": {
        "type_name": "Muon",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
}


@producer(
    uses={category_ids, normalization_weights},
    produces={category_ids, normalization_weights},
)
def default(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # category ids
    events = self[category_ids](events, **kwargs)

    # mc-only weights
    if self.dataset_inst.is_mc:
        # normalization weights
        events = self[normalization_weights](events, **kwargs)

    return events
