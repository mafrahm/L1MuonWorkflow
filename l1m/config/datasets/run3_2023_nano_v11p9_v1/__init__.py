# coding: utf-8

"""
Common, analysis independent definition of the 2023 data-taking campaign
with datasets at NanoAOD tier in version 11p9_v1.
See https://python-order.readthedocs.io/en/latest/quickstart.html#analysis-campaign-and-config.

Dataset ids are identical to those in DAS (https://cmsweb.cern.ch/das).
"""

from order import Campaign


#
# campaign
#

campaign_run3_2023_nano_v11p9_v1 = Campaign(
    name="run3_2023_nano_v11p9_v1",
    id=320231,
    ecm=13.6,  # NOTE: might clash with process xsecs as they are only implemented for 13 TeV
    bx=25,  # ?
    aux={"year": 2023, "tier": "NanoAOD", "version": "11p9_v1"},
)


# trailing imports to load datasets
import l1m.config.datasets.run3_2023_nano_v11p9_v1.data  # noqa
