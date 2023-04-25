# coding: utf-8

"""
Tasks related to reducing events for use on further tasks.
"""

from collections import defaultdict

import law

from l1m.tasks.base import L1MTask
from columnflow.tasks.framework.base import Requirements, DatasetTask
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ChunkedIOMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.tasks.selection import CalibrateEvents
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox


ak = maybe_import("awkward")


class CustomReduceEvents(
    L1MTask,
    SelectorStepsMixin,
    CalibratorsMixin,
    ChunkedIOMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        GetDatasetLFNs=GetDatasetLFNs,
        CalibrateEvents=CalibrateEvents,
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["lfns"] = self.reqs.GetDatasetLFNs.req(self)

        if not self.pilot:
            reqs["calib"] = [
                self.reqs.CalibrateEvents.req(self, calibrator=c)
                for c in self.calibrators
            ]
        else:
            # pass-through pilot workflow requirements of upstream task
            t = self.reqs.CalibrateEvents.req(self)
            reqs = law.util.merge_dicts(reqs, t.workflow_requires(), inplace=True)

        # add selector dependent requirements
        reqs["selector"] = self.selector_inst.run_requires()

        return reqs

    def requires(self):
        reqs = {
            "lfns": self.reqs.GetDatasetLFNs.req(self),
            "calibrations": [
                self.reqs.CalibrateEvents.req(self, calibrator=c)
                for c in self.calibrators
            ],
        }

        # add selector dependent requirements
        reqs["selector"] = self.selector_inst.run_requires()

        return reqs

    def output(self):
        outputs = {
            "events": self.target(f"events_{self.branch}.parquet"),
            "stats": self.target(f"stats_{self.branch}.json"),
        }
        return outputs

    @law.decorator.log
    @ensure_proxy
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        reqs = self.requires()
        lfn_task = reqs["lfns"]
        inputs = self.input()
        outputs = self.output()
        column_chunks = {}
        stats = defaultdict(float)

        # run the selector setup
        self.selector_inst.run_setup(reqs["selector"], inputs["selector"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that need to be read
        read_columns = mandatory_coffea_columns | self.selector_inst.used_columns | set(aliases.values())
        read_columns = {Route(c) for c in read_columns}

        # define columns that will be written
        write_columns = {
            Route(c)
            for c in self.config_inst.x.keep_columns.get(self.task_family, ["*"])
        } | mandatory_coffea_columns | self.selector_inst.produced_columns
        route_filter = RouteFilter(write_columns)

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open the input file with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks of events and diffs
        n_calib = len(inputs["calibrations"])
        for (events, *diffs), pos in self.iter_chunked_io(
            [nano_file] + [inp.path for inp in inputs["calibrations"]],
            source_type=["coffea_root"] + n_calib * ["awkward_parquet"],
            read_columns=(1 + n_calib) * [read_columns],
        ):
            # apply the calibrated diffs
            events = update_ak_array(events, *diffs)

            # add aliases
            events = add_ak_aliases(events, aliases, remove_src=True)

            # invoke the selection function
            # NOTE: results not used at all at the moment
            events, results = self.selector_inst(events, stats)

            # remove columns
            if write_columns:
                events = route_filter(events)

                # optional check for finite values
                if self.check_finite:
                    self.raise_if_not_finite(events)

                # save additional columns as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"cols_{lfn_index}_{pos.index}.parquet", type="f")
                column_chunks[(lfn_index, pos.index)] = chunk
                self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.path))

        # merge the column files
        if write_columns:
            sorted_chunks = [column_chunks[key] for key in sorted(column_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, outputs["events"], local=True)

        # save stats
        outputs["stats"].dump(stats, indent=4, formatter="json")
