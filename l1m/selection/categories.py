# coding: utf-8

"""
Category selectors
"""

from columnflow.selection import Selector, selector
from columnflow.util import maybe_import


np = maybe_import("numpy")
ak = maybe_import("awkward")


#
# selector used by categories definitions
# (not "exposed" to be used from the command line)
#

@selector(uses={"event"})
def sel_incl(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    # return a mask with
    return ak.ones_like(events.event) == 1


@selector(uses={"Muon.pt"})
def sel_2mu(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.num(events.Muon.pt) >= 2
