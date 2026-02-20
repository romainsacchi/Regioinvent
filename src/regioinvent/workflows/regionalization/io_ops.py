import collections
import uuid

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


def write_database(regio, target_db_name=None):
    """
    Write the final in-memory database to Brightway as a single database.
    """

    if not getattr(regio, "_final_database_in_memory", None):
        raise ValueError(
            "No in-memory final database found. Run regionalize_ecoinvent_with_trade() first."
        )

    regio.target_db_name = target_db_name or f"{regio.source_db_name} - regionalized"

    regio.logger.info("Write in-memory database to brightway...")
    regio.logger.info("Normalizing in-memory datasets before write...")

    final_data = {
        (ds["database"], ds["code"]): ds for ds in regio._final_database_in_memory
    }

    # Assign fresh UUID codes to every dataset and keep mapping from old -> new.
    old_to_new = {}
    code_to_new_candidates = collections.defaultdict(set)
    for old_key, ds in final_data.items():
        new_code = uuid.uuid4().hex
        old_to_new[old_key] = (regio.target_db_name, new_code)
        if old_key[1] is not None:
            code_to_new_candidates[old_key[1]].add(
                (regio.target_db_name, new_code)
            )
        ds["database"] = regio.target_db_name
        ds["code"] = new_code

    # Resolve code-only fallback only when unambiguous.
    code_to_new = {
        old_code: list(targets)[0]
        for old_code, targets in code_to_new_candidates.items()
        if len(targets) == 1
    }

    # Export as a single database: normalize links to target DB.
    normalized_data = {}
    for _, ds in final_data.items():
        for exc in ds["exchanges"]:
            if exc["type"] in ["technosphere", "production"]:
                exc["database"] = regio.target_db_name
                if exc["type"] == "production":
                    exc["code"] = ds["code"]
                    exc["input"] = (regio.target_db_name, ds["code"])
                else:
                    target = None
                    old_input = exc.get("input")
                    if isinstance(old_input, tuple) and len(old_input) == 2:
                        target = old_to_new.get((old_input[0], old_input[1]))
                    if target is None and "database" in exc and "code" in exc:
                        target = old_to_new.get((exc["database"], exc["code"]))
                    if target is None and "code" in exc:
                        target = code_to_new.get(exc["code"])
                    if target is not None:
                        exc["code"] = target[1]
                        exc["input"] = target
                    elif "code" in exc:
                        # Fallback: still populate an input in target database namespace.
                        exc["input"] = (regio.target_db_name, exc["code"])
            elif "input" not in exc and "database" in exc and "code" in exc:
                exc["input"] = (exc["database"], exc["code"])
            if exc["type"] == "production":
                exc["output"] = (regio.target_db_name, ds["code"])
        ds.pop("categories", None)
        ds.pop("parameters", None)
        normalized_data[(regio.target_db_name, ds["code"])] = ds

    if regio.target_db_name in bw2.databases:
        del bw2.databases[regio.target_db_name]

    regio.logger.info("Starting Brightway write...")
    bw2.Database(regio.target_db_name).write(normalized_data)


def connect_ecoinvent_to_regioinvent(regio):
    """
    Now that regioinvent exists, we can make ecoinvent use regioinvent processes to further deepen the
    regionalization. Only countries and sub-countries are connected to regioinvent, simply because in regioinvent
    we do not have consumption mixes for the different regions of ecoinvent (e.g., RER, RAS, etc.).
    However, Swiss processes are not affected, as ecoinvent was already tailored for the Swiss case.
    I am not sure regioinvent would bring more precision in that specific case.
    """

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

    for process in regio.ei_wurst:
        # find country/sub-country locations for process, we ignore regions
        location = None
        # for countries (e.g., CA)
        if process["location"] in regio.country_to_ecoinvent_regions.keys():
            location = process["location"]
        # for sub-countries (e.g., CA-QC)
        elif (
            process["location"].split("-")[0]
            in regio.country_to_ecoinvent_regions.keys()
        ):
            location = process["location"].split("-")[0]
        # check if location is not None and not Switzerland
        if location and location != "CH":
            # loop through technosphere exchanges
            for exc in process["exchanges"]:
                if exc.get("type") != "technosphere":
                    continue
                # if the product of the exchange is among the internationally traded commodities
                if exc["product"] in regio.eco_to_hs_class.keys():
                    # get the name of the corresponding consumtion market
                    exc["name"] = "consumption market for " + exc["product"]
                    # get the location of the process
                    exc["location"] = location
                    # if the consumption market exists for the process location
                    if (
                        "consumption market for " + exc["product"],
                        location,
                    ) in consumption_markets_data.keys():
                        exc["database"] = consumption_markets_data[
                            ("consumption market for " + exc["product"], location)
                        ]["database"]
                        exc["code"] = consumption_markets_data[
                            ("consumption market for " + exc["product"], location)
                        ]["code"]
                    # if the consumption market does not exist for the process location, take RoW
                    else:
                        exc["database"] = consumption_markets_data[
                            ("consumption market for " + exc["product"], "RoW")
                        ]["database"]
                        exc["code"] = consumption_markets_data[
                            ("consumption market for " + exc["product"], "RoW")
                        ]["code"]
                    exc["input"] = (exc["database"], exc["code"])
                # if the product of the exchange is among the non-international traded commodities
                elif (
                    exc["product"] in regionalized_products
                    and exc["product"] not in regio.eco_to_hs_class.keys()
                ):
                    tech_key = ("technology mix for " + exc["product"], location)
                    if tech_key not in techno_mixes:
                        tech_key = ("technology mix for " + exc["product"], "RoW")
                    if tech_key in techno_mixes:
                        exc["code"] = techno_mixes[tech_key]
                        exc["database"] = regio.target_db_name
                        exc["name"] = tech_key[0]
                        exc["location"] = tech_key[1]
                        exc["input"] = (exc["database"], exc["code"])

    # aggregating duplicate inputs (e.g., multiple consumption markets RoW callouts)
    for process in regio.ei_wurst:
        # Some technosphere exchanges can still miss an input tuple at this stage.
        # Reconstruct from available database/code metadata before deduplication.
        for exc in process["exchanges"]:
            if (
                exc.get("type") == "technosphere"
                and "input" not in exc
                and "database" in exc
                and "code" in exc
            ):
                exc["input"] = (exc["database"], exc["code"])

        duplicates = [
            item
            for item, count in collections.Counter(
                [
                    (
                        i["input"],
                        i["name"],
                        i["product"],
                        i["location"],
                        i["database"],
                        i["code"],
                    )
                    for i in process["exchanges"]
                    if i.get("type") == "technosphere"
                ]
            ).items()
            if count > 1
        ]

        for duplicate in duplicates:
            total = sum(
                [
                    i["amount"]
                    for i in process["exchanges"]
                    if i.get("type") == "technosphere" and i["input"] == duplicate[0]
                ]
            )
            process["exchanges"] = [
                i
                for i in process["exchanges"]
                if not (i.get("type") == "technosphere" and i["input"] == duplicate[0])
            ]
            process["exchanges"].append(
                {
                    "amount": total,
                    "type": "technosphere",
                    "input": duplicate[0],
                    "name": duplicate[1],
                    "product": duplicate[2],
                    "location": duplicate[3],
                    "database": duplicate[4],
                    "code": duplicate[5],
                }
            )

    # we also change production processes of ecoinvent for regionalized production processes of regioinvent
    regio_dict = {
        (
            i["reference product"],
            i["name"],
            i["location"],
        ): i["code"]
        for i in regio.regioinvent_in_wurst
    }

    for process in regio.ei_wurst:
        for exc in process["exchanges"]:
            if exc.get("type") != "technosphere":
                continue
            if exc["product"] in regio.eco_to_hs_class.keys():
                # same thing, we don't touch Swiss processes
                if exc["location"] not in ["RoW", "CH"]:
                    match_key = (exc["product"], exc["name"], exc["location"])
                    if match_key in regio_dict:
                        exc["database"] = regio.target_db_name
                        exc["code"] = regio_dict[match_key]
                        exc["input"] = (exc["database"], exc["code"])

    # Build final in-memory database that can later be written once.
    regio._final_database_in_memory = list(regio.ei_wurst) + list(regio.regioinvent_in_wurst)


def write_regioinvent_to_database(regio):
    """Backward-compatible alias for write_database()."""
    return write_database(regio)
