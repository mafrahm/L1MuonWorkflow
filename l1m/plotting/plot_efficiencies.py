# coding: utf-8

"""
Example plot functions for one-dimensional plots.
"""

from __future__ import annotations

from collections import OrderedDict
# from typing import Iterable

# import law

from columnflow.util import maybe_import
# from columnflow.plotting.plot_all import plot_all
from columnflow.plotting.plot_util import (
    # prepare_plot_config,
    # prepare_style_config,
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
    apply_density_to_hists,
)


hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


cms_label_kwargs = {"label": "Work in Progress", "fontsize": 22, "data": True}


def plot_eff(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool | None = False,
    yscale: str | None = "",
    hide_errors: bool | None = None,
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    Plot function to create efficiency plots
    """

    from hist.intervals import clopper_pearson_interval

    remove_residual_axis(hists, "shift")

    variable_inst = variable_insts[0]

    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

    # add all processes into 1 histogram
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())

    # use CMS plotting style
    plt.style.use(mplhep.style.CMS)
    fig, ax = plt.subplots()

    # get the histogram axis of the variable
    var_axis = h_sum.axes[variable_inst.name]

    # get the category instances
    denom_cat = config_inst.get_category("valid_probe")
    categories = [
        h_sum.axes["category"].bin(i)
        for i in range(h_sum.axes["category"].size)
    ]
    category_insts = [
        config_inst.get_category(c) for c in categories
        if c != denom_cat.id
    ]

    for category_inst in category_insts:
        h_num = h_sum[{"category": hist.loc(category_inst.id)}].values()
        h_denom = h_sum[{"category": hist.loc(denom_cat.id)}].values()
        # vals = h_num / h_denom
        vals = np.divide(h_num, h_denom, out=np.zeros_like(h_num), where=h_denom != 0)
        yerr = clopper_pearson_interval(h_num, h_denom)
        yerr = np.where(vals == 0, 0, np.abs(yerr - vals))
        xerr = (var_axis.edges[1:] - var_axis.edges[:-1]) / 2

        plot_kwargs = {
            "x": var_axis.centers, "y": vals,
            "yerr": yerr, "xerr": xerr,
            "label": category_inst.label,
            "linestyle": "none", "marker": "D",
            "markersize": 7, "elinewidth": 2,
            "capsize": 3,
        }
        ax.errorbar(**plot_kwargs)

    # plot styling
    ax.grid(color="grey", linestyle="--", linewidth=0.5)
    ax.set_yticks(ticks=np.arange(0, 1.51, 0.1))
    ax.set(
        xlim=(variable_inst.x_min, variable_inst.x_max),
        ylim=(0.0, 1.01),
        ylabel="L1T Efficiency",
        xlabel=variable_inst.x_title,
        xscale="log" if variable_inst.x_log else "linear",
        yscale="log" if variable_inst.y_log else "linear",
    )

    # labels and legend
    mplhep.cms.label(ax=ax, **cms_label_kwargs)
    ax.legend(loc="best", title="Trigger", fontsize=20, title_fontsize=24)

    plt.tight_layout()

    return fig, (ax,)
