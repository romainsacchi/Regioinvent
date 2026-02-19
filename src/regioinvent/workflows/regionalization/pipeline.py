import sqlite3

import brightway2 as bw2


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

    if cutoff > 0.99 or cutoff < 0:
        raise KeyError("cutoff must be between 0 and 0.99")

    if regio.name_ei_with_regionalized_biosphere not in bw2.databases:
        raise KeyError("You need to run the function spatialize_my_ecoinvent() first.")

    if not regio.ei_wurst:
        regio.ei_wurst = regio._extract_brightway2_databases(
            regio.name_ei_with_regionalized_biosphere
        )
    if not regio.ei_in_dict:
        regio.ei_in_dict = {
            (i["reference product"], i["location"], i["name"]): i for i in regio.ei_wurst
        }

    regio.format_trade_data()
    regio.first_order_regionalization()
    regio.create_consumption_markets()
    regio.second_order_regionalization()
    regio.spatialize_elem_flows()
    regio.write_regioinvent_to_database()
    regio.connect_ecoinvent_to_regioinvent()
