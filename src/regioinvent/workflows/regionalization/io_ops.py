import collections

import brightway2 as bw2
import pandas as pd


def format_trade_data(regio):
    """
    Function extracts and formats the export/import and domestic production data from the trade database
    :return: self.production_data / self.consumption_data
    """

    regio.logger.info("Extracting and formatting trade data...")

    # load import data corrected for re-exports
    import_data = pd.read_sql("SELECT * FROM [Import data]", regio.trade_conn).drop(
        "source", axis=1
    )

    # load export data (that's actually net exports, as in exports - imports)
    net_exports_data = pd.read_sql("SELECT * FROM [Export data]", regio.trade_conn).drop(
        "source", axis=1
    )

    # load domestic production
    regio.domestic_production = pd.read_sql(
        "SELECT * FROM [Domestic production data]", regio.trade_conn
    )

    # concatenate import and domestic data into consumption data
    regio.consumption_data = pd.concat(
        [import_data, regio.domestic_production.drop("source", axis=1)]
    )

    # concatenate net exports and domestic data into production data
    regio.production_data = pd.concat(
        [
            net_exports_data,
            regio.domestic_production.drop(["source", "importer"], axis=1),
        ]
    )
    regio.production_data = (
        regio.production_data.groupby(["cmdCode", "refYear", "exporter"]).sum().reset_index()
    )


def write_regioinvent_to_database(regio):
    """
    Function write a dictionary of datasets to the brightway2 SQL database
    """

    regio.logger.info("Write regioinvent database to brightway...")

    # change regioinvent data from wurst to bw2 structure
    regioinvent_data = {(i["database"], i["code"]): i for i in regio.regioinvent_in_wurst}

    # recreate inputs in edges (exchanges)
    for pr in regioinvent_data:
        for exc in regioinvent_data[pr]["exchanges"]:
            try:
                exc["input"]
            except KeyError:
                exc["input"] = (exc["database"], exc["code"])
    # wurst creates empty categories for activities, this creates an issue when you try to write the bw2 database
    for pr in regioinvent_data:
        try:
            del regioinvent_data[pr]["categories"]
        except KeyError:
            pass

    # write regioinvent database in brightway2
    bw2.Database(regio.regioinvent_database_name).write(regioinvent_data)


def connect_ecoinvent_to_regioinvent(regio):
    """
    Now that regioinvent exists, we can make ecoinvent use regioinvent processes to further deepen the
    regionalization. Only countries and sub-countries are connected to regioinvent, simply because in regioinvent
    we do not have consumption mixes for the different regions of ecoinvent (e.g., RER, RAS, etc.).
    However, Swiss processes are not affected, as ecoinvent was already tailored for the Swiss case.
    I am not sure regioinvent would bring more precision in that specific case.
    """

    # Here we are directly manipulating (through bw2) the already-written ecoinvent database [self.name_ei_with_regionalized_biosphere]
    regio.logger.info("Connecting ecoinvent to regioinvent processes...")

    # as dictionary to speed searching for information
    consumption_markets_data = {
        (i["name"], i["location"]): i
        for i in regio.regioinvent_in_wurst
        if "consumption market" in i["name"]
    }
    regionalized_products = set([i["reference product"] for i in regio.regioinvent_in_wurst])
    techno_mixes = {
        (i["name"], i["location"]): i["code"]
        for i in regio.regioinvent_in_wurst
        if "technology mix" in i["name"]
    }

    for process in bw2.Database(regio.name_ei_with_regionalized_biosphere):
        # find country/sub-country locations for process, we ignore regions
        location = None
        # for countries (e.g., CA)
        if process.as_dict()["location"] in regio.country_to_ecoinvent_regions.keys():
            location = process.as_dict()["location"]
        # for sub-countries (e.g., CA-QC)
        elif (
            process.as_dict()["location"].split("-")[0]
            in regio.country_to_ecoinvent_regions.keys()
        ):
            location = process.as_dict()["location"].split("-")[0]
        # check if location is not None and not Switzerland
        if location and location != "CH":
            # loop through technosphere exchanges
            for exc in process.technosphere():
                # if the product of the exchange is among the internationally traded commodities
                if exc.as_dict()["product"] in regio.eco_to_hs_class.keys():
                    # get the name of the corresponding consumtion market
                    exc.as_dict()["name"] = "consumption market for " + exc.as_dict()["product"]
                    # get the location of the process
                    exc.as_dict()["location"] = location
                    # if the consumption market exists for the process location
                    if (
                        "consumption market for " + exc.as_dict()["product"],
                        location,
                    ) in consumption_markets_data.keys():
                        exc.as_dict()["database"] = consumption_markets_data[
                            ("consumption market for " + exc.as_dict()["product"], location)
                        ]["database"]
                        exc.as_dict()["code"] = consumption_markets_data[
                            ("consumption market for " + exc.as_dict()["product"], location)
                        ]["code"]
                    # if the consumption market does not exist for the process location, take RoW
                    else:
                        exc.as_dict()["database"] = consumption_markets_data[
                            ("consumption market for " + exc.as_dict()["product"], "RoW")
                        ]["database"]
                        exc.as_dict()["code"] = consumption_markets_data[
                            ("consumption market for " + exc.as_dict()["product"], "RoW")
                        ]["code"]
                    exc.as_dict()["input"] = (exc.as_dict()["database"], exc.as_dict()["code"])
                    exc.save()
                # if the product of the exchange is among the non-international traded commodities
                elif (
                    exc.as_dict()["product"] in regionalized_products
                    and exc.as_dict()["product"] not in regio.eco_to_hs_class.keys()
                ):
                    try:
                        # if techno mix for location exists
                        exc.as_dict()["code"] = techno_mixes[
                            ("technology mix for " + exc.as_dict()["product"], location)
                        ]
                        exc.as_dict()["database"] = regio.regioinvent_database_name
                        exc.as_dict()["name"] = "technology mix for " + exc.as_dict()["product"]
                        exc.as_dict()["location"] = location
                        exc.as_dict()["input"] = (
                            exc.as_dict()["database"],
                            exc.as_dict()["code"],
                        )
                        exc.save()
                    except KeyError:
                        # if not, link to RoW
                        exc.as_dict()["code"] = techno_mixes[
                            ("technology mix for " + exc.as_dict()["product"], "RoW")
                        ]
                        exc.as_dict()["database"] = regio.regioinvent_database_name
                        exc.as_dict()["name"] = "technology mix for " + exc.as_dict()["product"]
                        exc.as_dict()["location"] = "RoW"
                        exc.as_dict()["input"] = (exc.as_dict()["database"], exc.as_dict()["code"])
                        exc.save()

    # aggregating duplicate inputs (e.g., multiple consumption markets RoW callouts)
    for process in bw2.Database(regio.name_ei_with_regionalized_biosphere):
        duplicates = [
            item
            for item, count in collections.Counter(
                [
                    (
                        i.as_dict()["input"],
                        i.as_dict()["name"],
                        i.as_dict()["product"],
                        i.as_dict()["location"],
                        i.as_dict()["database"],
                        i.as_dict()["code"],
                    )
                    for i in process.technosphere()
                ]
            ).items()
            if count > 1
        ]

        for duplicate in duplicates:
            total = sum([i["amount"] for i in process.technosphere() if i["input"] == duplicate[0]])
            [i.delete() for i in process.technosphere() if i["input"] == duplicate[0]]
            new_exc = process.new_exchange(
                amount=total,
                type="technosphere",
                input=duplicate[0],
                name=duplicate[1],
                product=duplicate[2],
                location=duplicate[3],
                database=duplicate[4],
                code=duplicate[5],
            )
            new_exc.save()

    # we also change production processes of ecoinvent for regionalized production processes of regioinvent
    regio_dict = {
        (
            i.as_dict()["reference product"],
            i.as_dict()["name"],
            i.as_dict()["location"],
        ): i
        for i in bw2.Database(regio.regioinvent_database_name)
    }

    for process in bw2.Database(regio.name_ei_with_regionalized_biosphere):
        for exc in process.technosphere():
            if exc.as_dict()["product"] in regio.eco_to_hs_class.keys():
                # same thing, we don't touch Swiss processes
                if exc.as_dict()["location"] not in ["RoW", "CH"]:
                    try:
                        exc.as_dict()["database"] = regio.regioinvent_database_name
                        exc.as_dict()["code"] = regio_dict[
                            (
                                exc.as_dict()["product"],
                                exc.as_dict()["name"],
                                exc.as_dict()["location"],
                            )
                        ].as_dict()["code"]
                        exc.as_dict()["input"] = (exc.as_dict()["database"], exc.as_dict()["code"])
                    except KeyError:
                        pass
