# coding: utf-8

"""
Configuration of the L1MuonWorkflow analysis.
"""

import os
import functools

import law
import order as od
from scinum import Number

from columnflow.util import DotDict, maybe_import
from columnflow.config_util import (
    get_root_processes_from_campaign, add_shift_aliases, get_shifts_from_sources, add_category,
)
from columnflow.tasks.external import GetDatasetLFNs

from l1m.config.trigger import add_trigger_categories
from l1m.config.variables import add_probe_variables

ak = maybe_import("awkward")

#
# the main analysis object
#

analysis_l1m = ana = od.Analysis(
    name="analysis_l1m",
    id=1,
)

# analysis-global versions
ana.x.versions = {}

# files of bash sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.bash_sandboxes = [
    "$CF_BASE/sandboxes/cf_prod.sh",
    "$CF_BASE/sandboxes/venv_columnar.sh",
]

# files of cmssw sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.cmssw_sandboxes = [
    # "$CF_BASE/sandboxes/cmssw_default.sh",
]

# clear the list when cmssw bundling is disabled
if not law.util.flag_to_bool(os.getenv("L1M_BUNDLE_CMSSW", "1")):
    del ana.x.cmssw_sandboxes[:]

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
ana.x.config_groups = {}


#
# setup configs
#

# an example config is setup below, based on cms NanoAOD v9 for Run2 2017, focussing on
# ttbar and single top MCs, plus single muon data
# update this config or add additional ones to accomodate the needs of your analysis

# from cmsdb.campaigns.run2_2017_nano_v9 import campaign_run2_2017_nano_v9
from l1m.config.datasets.run3_2023_nano_v11p9_v1 import campaign_run3_2023_nano_v11p9_v1

# copy the campaign
# (creates copies of all linked datasets, processes, etc. to allow for encapsulated customization)
campaign = campaign_run3_2023_nano_v11p9_v1.copy()

# get all root processes
procs = get_root_processes_from_campaign(campaign)

# create a config by passing the campaign, so id and name will be identical
config = cfg = ana.add_config(campaign)

# gather campaign data
year = campaign.x.year

# add processes we are interested in
process_names = [
    "data",
    "data_mu",
]
for process_name in process_names:
    # add the process
    proc = cfg.add_process(procs.get(process_name))

    # configuration of colors, labels, etc. can happen here
    if proc.is_mc:
        proc.color1 = (244, 182, 66) if proc.name == "tt" else (244, 93, 66)

# add datasets we need to study
dataset_names = [
    "prompt_data_mu0",
    "prompt_data_mu1",
    # empty since we only add custom datasets at the moment
]
for dataset_name in dataset_names:
    # add the dataset
    dataset = cfg.add_dataset(campaign.get_dataset(dataset_name))

    # limit the number of files per dataset (for quick tests)
    limit_dataset_files = 999
    for info in dataset.info.values():
        info.n_files = min(info.n_files, limit_dataset_files)

# custom datasets (TODO: move in a separate file)
# add_custom_datasets(config)
cfg.add_dataset(
    name="l1_data_mu",
    id=1234569,
    is_data=True,
    processes=[cfg.get_process("data_mu")],
    info=dict(nominal=od.DatasetInfo(
        keys=["data_mu"],
        n_files=1,
        n_events=50000,
    )),
    aux={"custom": True},
)

# default objects, such as calibrator, selector, producer, ml model, inference model, etc
cfg.x.default_calibrator = None
cfg.x.default_selector = "muon_reduction"
cfg.x.default_producer = "default"
cfg.x.default_ml_model = None
cfg.x.default_inference_model = None
cfg.x.default_categories = ("incl",)
cfg.x.default_variables = ("n_jet", "jet1_pt")

# process groups for conveniently looping over certain processs
# (used in wrapper_factory and during plotting)
cfg.x.process_groups = {}

# dataset groups for conveniently looping over certain datasets
# (used in wrapper_factory and during plotting)
cfg.x.dataset_groups = {}

# category groups for conveniently looping over certain categories
# (used during plotting)
cfg.x.category_groups = {}

# variable groups for conveniently looping over certain variables
# (used during plotting)
cfg.x.variable_groups = {}

# shift groups for conveniently looping over certain shifts
# (used during plotting)
cfg.x.shift_groups = {}

# selector step groups for conveniently looping over certain steps
# (used in cutflow tasks)
cfg.x.selector_step_groups = {
    "default": ["muon", "jet"],
}

# custom method and sandbox for determining dataset lfns
cfg.x.get_dataset_lfns = None
cfg.x.get_dataset_lfns_sandbox = None

# whether to validate the number of obtained LFNs in GetDatasetLFNs
# (currently set to false because the number of files per dataset is truncated to 2)
cfg.x.validate_dataset_lfns = False

# lumi values in inverse pb
# https://twiki.cern.ch/twiki/bin/view/CMS/LumiRecommendationsRun2?rev=2#Combination_and_correlations
cfg.x.luminosity = Number(41480, {
    "lumi_13TeV_2017": 0.02j,
    "lumi_13TeV_1718": 0.006j,
    "lumi_13TeV_correlated": 0.009j,
})

# names of muon correction sets and working points
# (used in the muon producer)
cfg.x.muon_sf_names = ("NUM_TightRelIso_DEN_TightIDandIPCut", f"{year}_UL")

# register shifts
cfg.add_shift(name="nominal", id=0)

# tune shifts are covered by dedicated, varied datasets, so tag the shift as "disjoint_from_nominal"
# (this is currently used to decide whether ML evaluations are done on the full shifted dataset)
cfg.add_shift(name="tune_up", id=1, type="shape", tags={"disjoint_from_nominal"})
cfg.add_shift(name="tune_down", id=2, type="shape", tags={"disjoint_from_nominal"})

# fake jet energy correction shift, with aliases flaged as "selection_dependent", i.e. the aliases
# affect columns that might change the output of the event selection
cfg.add_shift(name="jec_up", id=20, type="shape")
cfg.add_shift(name="jec_down", id=21, type="shape")
add_shift_aliases(
    cfg,
    "jec",
    {
        "Jet.pt": "Jet.pt_{name}",
        "Jet.mass": "Jet.mass_{name}",
        "MET.pt": "MET.pt_{name}",
        "MET.phi": "MET.phi_{name}",
    },
)

# event weights due to muon scale factors
cfg.add_shift(name="mu_up", id=10, type="shape")
cfg.add_shift(name="mu_down", id=11, type="shape")
add_shift_aliases(cfg, "mu", {"muon_weight": "muon_weight_{direction}"})

# external files
json_mirror = "/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-849c6a6e"
cfg.x.external_files = DotDict.wrap({
    # lumi files
    "lumi": {
        "golden": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt", "v1"),  # noqa
        "normtag": ("/afs/cern.ch/user/l/lumipro/public/Normtags/normtag_PHYSICS.json", "v1"),
    },

    # muon scale factors
    "muon_sf": (f"{json_mirror}/POG/MUO/{year}_UL/muon_Z.json.gz", "v1"),
})

# target file size after MergeReducedEvents in MB
cfg.x.reduced_file_size = 512.0

# columns to keep after certain steps
cfg.x.keep_columns = DotDict.wrap({
    "cf.ReduceEvents": {
        # general event info
        "run", "luminosityBlock", "event",
        # object info
        # "Muon.pt", "Muon.eta", "Muon.phi", "Muon.mass", "Muon.pfRelIso04_all",
        # "L1Mu.pt", "L1Mu.eta", "L1Mu.phi", "L1Mu.mass", "L1Mu.hwQual", "L1Mu.bx",
        # "TagMuon.*", "ProbeMuon.*", "L1TagMuon.*"
        "PV.npvs",
        # columns added during selection
        "deterministic_seed", "process_id", "mc_weight",
        "N_probes",
    } | set(
        f"{l1mu}.{field}"
        for l1mu in ("L1Mu", "L1TagMuon")
        for field in ("pt", "eta", "phi", "mass", "hwQual", "bx")
    ) | set(
        f"{recomu}.{field}"
        for recomu in ("Muon", "TagMuon", "ProbeMuon")
        for field in ("pt", "eta", "phi", "mass")
    ),
    "cf.UniteColumns": {
        "*",
    },
})
cfg.x.keep_columns["l1m.CustomReduceEvents"] = cfg.x.keep_columns["cf.ReduceEvents"]

# event weight columns as keys in an OrderedDict, mapped to shift instances they depend on
get_shifts = functools.partial(get_shifts_from_sources, cfg)
cfg.x.event_weights = DotDict({
    "normalization_weight": [],
    # "muon_weight": get_shifts("mu"),
})

# versions per task family and optionally also dataset and shift
# None can be used as a key to define a default value
cfg.x.versions = {
    # "cf.CalibrateEvents": "prod1",
    # ...
}

# channels
# (just one for now)
cfg.add_channel(name="mutau", id=1)

# add categories using the "add_category" tool which adds auto-generated ids
# the "selection" entries refer to names of selectors, e.g. in selection/example.py
add_category(
    cfg,
    name="incl",
    id=1,
    selection="sel_incl",
    label="inclusive",
)
# trigger categories used for efficiency measurements
add_trigger_categories(cfg)

# add variables for histogramming/plotting
add_probe_variables(cfg)

cfg.add_variable(
    name="event",
    expression="event",
    binning=(1, 0.0, 1.0e9),
    x_title="Event number",
    discrete_x=True,
)
cfg.add_variable(
    name="run",
    expression="run",
    binning=(1, 100000.0, 500000.0),
    x_title="Run number",
    discrete_x=True,
)
cfg.add_variable(
    name="lumi",
    expression="luminosityBlock",
    binning=(1, 0.0, 5000.0),
    x_title="Luminosity block",
    discrete_x=True,
)
# weights
cfg.add_variable(
    name="mc_weight",
    expression="mc_weight",
    binning=(200, -10, 10),
    x_title="MC weight",
)


def get_dataset_lfns(
    dataset_inst: od.Dataset,
    shift_inst: od.Shift,
    dataset_key: str,
) -> list[str]:
    """
    Custom method to obtain dataset files
    """

    if not dataset_inst.x("custom", None):
        return GetDatasetLFNs.get_dataset_lfns_dasgoclient(
            GetDatasetLFNs, dataset_inst=dataset_inst, shift_inst=shift_inst, dataset_key=dataset_key,
        )
    print("dataset name:", dataset_inst.name)
    print("dataset_key:", dataset_key)

    # this just needs to give me the directory after the "location" in the campaign definition
    # so in my case just tt (the process)
    """
    lfn_base = law.wlcg.WLCGDirectoryTarget(
        "/" + dataset_key + "/",
        # fs="wlcg_fs_eos_frahm",
        # fs="wlcg_fs_run2_2017_nano_L1nano",
    )
    """
    lfn_base = law.LocalDirectoryTarget(
        f"/nfs/dust/cms/user/frahmmat/data/L1nano/{dataset_key}/",
        fs="local_nanos",
    )

    # loop though files and interpret paths as lfns
    return [
        lfn_base.child(basename, type="f").path
        for basename in lfn_base.listdir(pattern="*.root")
    ]


cfg.x.get_dataset_lfns = get_dataset_lfns
