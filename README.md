# L1T Muon Efficiencies

Analysis based on [columnflow](https://github.com/uhh-cms/columnflow), [law](https://github.com/riga/law) and [order](https://github.com/riga/order).

This analysis implements a custom [task structure](https://github.com/mafrahm/L1MuonWorkflow/wiki/Custom-Task-structure).

## Setup

```shell
# clone the project
git clone --recursive git@github.com:mafrahm/L1MuonWorkflow.git
cd L1MuonWorkflow

# source the setup and store decisions in .setups/dev.sh (arbitrary name)
source setup.sh dev

# index existing tasks once to enable auto-completion for "law run"
law index --verbose

# some tasks might require a working GRID proxy
voms-proxy-init -voms cms -rfc -valid 196:00
```

## Example commands
```shell
# Create an efficiency plot
law run cf.PlotVariables1D --version v1 --variables probe_pt --categories all_probes --plot-function l1m.plotting.plot_efficiencies.plot_eff --datasets prompt_data_mu0,prompt_data_mu1 --view-cmd imgcat
```


### Resources

- [columnflow](https://github.com/uhh-cms/columnflow)
- [law](https://github.com/riga/law)
- [order](https://github.com/riga/order)
- [luigi](https://github.com/spotify/luigi)
