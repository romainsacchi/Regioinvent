import sqlite3
from time import perf_counter

import brightway2 as bw2
import bw2data


def regionalize_ecoinvent_with_trade(
    regio, trade_database_path, regioinvent_database_name, cutoff
):
    """
    Function runs all the necessary sub-functions to incorporate trade data within ecoinvent supply chains
    descriptions
    :param trade_database_path: [str] the path to the trade database
    :param regioinvent_database_name: [str] the name to be given to the generated regioinvent database in brightway2
    :param cutoff: [float] the amount (between 0 and 1) after which exports/imports values of countries will be aggregated
                    into a Rest-of-theWorld aggregate.
    :return:
    """

    regio.trade_conn = sqlite3.connect(trade_database_path)
    regio.regioinvent_database_name = regioinvent_database_name
    regio.cutoff = cutoff

    if regioinvent_database_name in bw2data.databases:
        regio.logger.info(
            f"Database '{regioinvent_database_name}' already exists; deleting it before regeneration."
        )
        del bw2data.databases[regioinvent_database_name]

    if cutoff > 0.99 or cutoff < 0:
        raise KeyError("cutoff must be between 0 and 0.99")

    if regio.name_ei_with_regionalized_biosphere not in bw2.databases:
        raise KeyError("You need to run the function spatialize_my_ecoinvent() first.")

    if not regio.ei_wurst:
        try:
            regio.ei_wurst = regio._extract_brightway2_databases(
                regio.name_ei_with_regionalized_biosphere
            )
        except Exception as exc:
            # Existing "ecoinvent ... regionalized" may still contain links to a previously
            # deleted regioinvent database. Rebuild it from pristine ecoinvent and retry.
            if "ActivityDatasetDoesNotExist" in str(type(exc)) or regioinvent_database_name in str(exc):
                regio.logger.warning(
                    "Detected dangling technosphere links in the regionalized ecoinvent "
                    "database after deleting the regioinvent database. Rebuilding "
                    "the regionalized ecoinvent copy and retrying extraction."
                )
                if regio.name_ei_with_regionalized_biosphere in bw2data.databases:
                    del bw2data.databases[regio.name_ei_with_regionalized_biosphere]
                regio.spatialize_my_ecoinvent()
                regio.ei_wurst = regio._extract_brightway2_databases(
                    regio.name_ei_with_regionalized_biosphere
                )
            else:
                raise
    if not regio.ei_in_dict:
        regio.ei_in_dict = {
            (i["reference product"], i["location"], i["name"]): i for i in regio.ei_wurst
        }

    t0 = perf_counter()
    stages = [
        ("format_trade_data", regio.format_trade_data),
        ("first_order_regionalization", regio.first_order_regionalization),
        ("create_consumption_markets", regio.create_consumption_markets),
        ("second_order_regionalization", regio.second_order_regionalization),
        ("spatialize_elem_flows", regio.spatialize_elem_flows),
        ("write_regioinvent_to_database", regio.write_regioinvent_to_database),
        ("connect_ecoinvent_to_regioinvent", regio.connect_ecoinvent_to_regioinvent),
    ]

    for stage_name, stage_fn in stages:
        ts = perf_counter()
        stage_fn()
        regio.logger.info(
            f"Stage timing - {stage_name}: {perf_counter() - ts:.2f}s"
        )

    regio.logger.info(
        f"Stage timing - regionalize_ecoinvent_with_trade total: {perf_counter() - t0:.2f}s"
    )
