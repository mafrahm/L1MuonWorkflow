# coding: utf-8

"""
Definition of variables.
"""

import order as od

from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


def add_probe_variables(config: od.Config) -> None:

    config.add_variable(
        name="probe_pt",
        expression="ProbeMuon.pt",
        binning=[0, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 18, 20, 22, 25, 30, 35, 45, 60, 75, 100, 140, 200],
        unit="GeV",
        x_title=r"Probe Muon $p_{T}$",
    )
    config.add_variable(
        name="probe_pt_bins1",
        expression="ProbeMuon.pt",
        binning=(40, 0., 200.),
        unit="GeV",
        x_title=r"Probe Muon $p_{T}$",
    )
    config.add_variable(
        name="probe_phi",
        expression="ProbeMuon.phi",
        binning=(40, -3.2, 3.2),
        x_title=r"Probe Muon $\phi$",
    )
    config.add_variable(
        name="probe_eta",
        expression="ProbeMuon.eta",
        binning=(50, -2.5, 2.5),
        x_title=r"Probe Muon $\eta$",
    )
    config.add_variable(
        name="probe_mass",
        expression="ProbeMuon.mass",
        binning=(40, 0, 200),
        x_title="Probe Muon mass",
    )
