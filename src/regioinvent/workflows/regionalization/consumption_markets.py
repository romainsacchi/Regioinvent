import collections
import copy
import json
import uuid
from importlib.resources import as_file, files

import brightway2 as bw2
import pandas as pd
import wurst.searching as ws
from tqdm import tqdm

def create_consumption_markets(regio):
    """
    Function creating consumption markets for each regionalized process
    :return:  regio.regioinvent_in_wurst with new regionalized processes
    """

    regio.logger.info(
        "Creating consumption markets for internationally-traded products..."
    )

    # change to dictionary to speed searching for info
    regio.regioinvent_in_dict = {
        tech: []
        for tech in [
            (i["reference product"], i["location"])
            for i in regio.regioinvent_in_wurst
        ]
    }
    # populate the empty dictionary
    for process in regio.regioinvent_in_wurst:
        regio.regioinvent_in_dict[
            (process["reference product"], process["location"])
        ].append({process["name"]: process})

    for product in tqdm(regio.eco_to_hs_class, leave=True):
        # filter the product in regio.consumption_data
        cmd_consumption_data = regio.consumption_data[
            regio.consumption_data.cmdCode == regio.eco_to_hs_class[product]
        ].copy("deep")
        # calculate the average consumption volume for each country
        cmd_consumption_data = cmd_consumption_data.groupby(
            ["importer", "exporter"]
        ).agg({"quantity (t)": "mean"})
        # change to relative values
        consumers = (
            cmd_consumption_data.groupby(level=0).sum()
            / cmd_consumption_data.sum().sum()
        ).sort_values(by="quantity (t)", ascending=False)
        # only keep consumers till the user-defined cut-off of total consumption
        limit = (
            consumers.index.get_loc(
                consumers[consumers.cumsum() > regio.cutoff].dropna().index[0]
            )
            + 1
        )
        # aggregate the rest
        remainder = (
            cmd_consumption_data.loc[consumers.index[limit:]].groupby(level=1).sum()
        )
        cmd_consumption_data = cmd_consumption_data.loc[consumers.index[:limit]]
        # assign the aggregate to RoW location
        cmd_consumption_data = pd.concat(
            [cmd_consumption_data, pd.concat([remainder], keys=["RoW"])]
        )
        cmd_consumption_data.index = pd.MultiIndex.from_tuples(
            [i for i in cmd_consumption_data.index]
        )
        cmd_consumption_data = cmd_consumption_data.sort_index()

        # loop through each selected consumers of the commodity
        for consumer in cmd_consumption_data.index.levels[0]:
            # change to relative values
            cmd_consumption_data.loc[consumer, "quantity (t)"] = (
                cmd_consumption_data.loc[consumer, "quantity (t)"]
                / cmd_consumption_data.loc[consumer, "quantity (t)"].sum()
            ).values
            # we need to add the aggregate to potentially already existing RoW exchanges
            cmd_consumption_data = pd.concat(
                [
                    cmd_consumption_data.drop("RoW", level=0),
                    pd.concat(
                        [cmd_consumption_data.loc["RoW"].groupby(level=0).sum()],
                        keys=["RoW"],
                    ),
                ]
            )
            cmd_consumption_data = cmd_consumption_data.fillna(0)

            try:
                source = (
                    regio.domestic_production.loc[
                        regio.domestic_production.cmdCode
                        == regio.eco_to_hs_class[product],
                        "source",
                    ]
                    .iloc[0]
                    .split(" - ")[0]
                )
            # if IndexError -> product is only consumed domestically and not exported according to exiobase
            except IndexError:
                source = "EXIOBASE"

            # create the process information
            new_import_data = {
                "name": "consumption market for " + product,
                "reference product": product,
                "location": consumer,
                "type": "process",
                "unit": regio.unit[product],
                "code": uuid.uuid4().hex,
                "comment": f"""This process represents the consumption market of {product} in {consumer}. The shares were determined based on two aspects. The imports of the commodity {regio.eco_to_hs_class[product]} taken from the BACI database (average over the years 2018, 2019, 2020, 2021, 2022). The domestic consumption data was extracted/estimated from {source}.""",
                "database": regio.regioinvent_database_name,
                "exchanges": [],
            }

            # create the production exchange
            new_import_data["exchanges"].append(
                {
                    "amount": 1,
                    "type": "production",
                    "input": (
                        regio.regioinvent_database_name,
                        new_import_data["code"],
                    ),
                }
            )
            # identify regionalized processes that were created in regio.first_order_regionalization()
            available_trading_partners = regio.created_geographies[product]
            # loop through the selected consumers
            for trading_partner in cmd_consumption_data.loc[consumer].index:
                # check if a regionalized process exist for that consumer
                if trading_partner in available_trading_partners:
                    # loop through available technologies to produce the commodity
                    for technology in regio.distribution_technologies[product]:
                        # get the uuid
                        code = [
                            i
                            for i in regio.regioinvent_in_dict[
                                (product, trading_partner)
                            ]
                            if list(i.keys())[0] == technology
                        ][0][technology]["code"]
                        # get the share
                        share = regio.distribution_technologies[product][technology]
                        # create the exchange
                        new_import_data["exchanges"].append(
                            {
                                "amount": cmd_consumption_data.loc[
                                    (consumer, trading_partner), "quantity (t)"
                                ]
                                * share,
                                "type": "technosphere",
                                "input": (regio.regioinvent_database_name, code),
                                "name": product,
                            }
                        )
                # if a regionalized process does not exist for consumer, take the RoW aggregate
                else:
                    # loop through available technologies to produce the commodity
                    for technology in regio.distribution_technologies[product]:
                        # get the uuid
                        code = [
                            i
                            for i in regio.regioinvent_in_dict[(product, "RoW")]
                            if list(i.keys())[0] == technology
                        ][0][technology]["code"]
                        # get the share
                        share = regio.distribution_technologies[product][technology]
                        # create the exchange
                        new_import_data["exchanges"].append(
                            {
                                "amount": cmd_consumption_data.loc[
                                    (consumer, trading_partner), "quantity (t)"
                                ]
                                * share,
                                "type": "technosphere",
                                "input": (regio.regioinvent_database_name, code),
                                "name": product,
                            }
                        )
            # add transportation to consumption market
            for transportation_mode in regio.transportation_modes[product]:
                new_import_data["exchanges"].append(
                    {
                        "amount": regio.transportation_modes[product][
                            transportation_mode
                        ],
                        "type": "technosphere",
                        "input": (
                            regio.name_ei_with_regionalized_biosphere,
                            transportation_mode,
                        ),
                    }
                )

            # check for duplicate input codes with different values (coming from RoW)
            duplicates = [
                item
                for item, count in collections.Counter(
                    [i["input"] for i in new_import_data["exchanges"]]
                ).items()
                if count > 1
            ]
            # add duplicates into one single flow
            for duplicate in duplicates:
                total = sum(
                    [
                        i["amount"]
                        for i in new_import_data["exchanges"]
                        if i["input"] == duplicate
                    ]
                )
                new_import_data["exchanges"] = [
                    i
                    for i in new_import_data["exchanges"]
                    if i["input"] != duplicate
                ] + [
                    {
                        "amount": total,
                        "name": product,
                        "type": "technosphere",
                        "input": duplicate,
                    }
                ]
            # add to database in wurst
            regio.regioinvent_in_wurst.append(new_import_data)
