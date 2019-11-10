"""Contains OpenDSS utility functions."""

import logging
import re


logger = logging.getLogger(__name__)


def get_pv_controllers(filename):
    """Return PV controllers specified in OpenDSS deployment file.

    Parameters
    ----------
    filename : str

    Returns
    -------
    list

    """
    controllers = set()
    """
    New PVSystem.pv_1114018 bus1=133294_xfmr.1.2 phases=2
    """
    regex = re.compile(r"^New PVSystem\.(\w+)\s")

    with open(filename) as fp_in:
        for line in fp_in:
            match = regex.search(line)
            if match:
                controllers.add(match.group(1))

    controllers = list(controllers)
    logger.debug("Found controllers=%s in %s", controllers, filename)
    return controllers


def read_capacitor_changes(event_log):
    """Read the capacitor state changes from an OpenDSS event log.

    Parameters
    ----------
    event_log : str
        Path to event log

    Returns
    -------
    dict
        Maps capacitor names to count of state changes.

    """
    capacitor_changes = {}
    regex = re.compile(r"(Capacitor\.\w+)")

    data = read_event_log(event_log)
    for row in data:
        match = regex.search(row["Element"])
        if match:
            name = match.group(1)
            if name not in capacitor_changes:
                capacitor_changes[name] = 0
            action = row["Action"].replace("*", "")
            if action in ("OPENED", "CLOSED", "STEP UP"):
                capacitor_changes[name] += 1

    return capacitor_changes


def read_event_log(filename):
    """Return OpenDSS event log information.

    Parameters
    ----------
    filename : str
        path to event log file.


    Returns
    -------
    list
        list of dictionaries (one dict for each row in the file)

    """
    data = []

    with open(filename) as f_in:
        for line in f_in:
            tokens = [x.strip() for x in line.split(",")]
            row = {}
            for token in tokens:
                name_and_value = [x.strip() for x in token.split("=")]
                name = name_and_value[0]
                value = name_and_value[1]
                row[name] = value
            data.append(row)

    return data
