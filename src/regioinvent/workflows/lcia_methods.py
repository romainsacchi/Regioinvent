from importlib.resources import as_file, files

import brightway2 as bw2


def import_fully_regionalized_impact_method(regio, lcia_method="all"):
    """
    Function to import a fully regionalized impact method into your brightway project, to-be-used with the
    spatialized version of ecoinvent. You can choose between IMPACT World+, EF and ReCiPe, or simply all of them.

    :param lcia_method: [str] the name of the LCIA method to be imported to be used with the spatialized ecoinvent,
                            available methods are "IW v2.1", "EF v3.1", "ReCiPe 2016 v1.03 (H)" or "all".
    :return:
    """

    if lcia_method not in ["IW v2.1", "EF v3.1", "ReCiPe 2016 v1.03 (H)", "all"]:
        raise KeyError(
            "Available LCIA methods are: 'IW v2.1', 'EF v3.1', 'ReCiPe 2016 v1.03 (H)' or 'all'"
        )

    # just load the correct BW2Package file from Data storage folder
    if lcia_method == "all" and regio.ecoinvent_version == "3.10":
        regio.logger.info("Importing all available fully regionalized lcia methods for ecoinvent3.10.")

        with as_file(
            files("regioinvent").joinpath(
                "data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v310.0fffd5e3daa5f4cf11ef83e49c375827.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

        with as_file(
            files("regioinvent").joinpath(
                "data/EF/EF31_regionalized-for-ecoinvent-v310.87ec66ed7e5775d0132d1129fb5caf03.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

        with as_file(
            files("regioinvent").joinpath(
                "data/ReCiPe/ReCiPe_regionalized-for-ecoinvent-v310.dd7e66b1994d898394e3acfbed8eef83.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

    if lcia_method == "all" and regio.ecoinvent_version == "3.9":
        regio.logger.info("Importing all available fully regionalized lcia methods for ecoinvent3.9.")

        with as_file(
            files("regioinvent").joinpath(
                "data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v39.af770e84bfd0f4365d509c026796639a.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

        with as_file(
            files("regioinvent").joinpath(
                "data/EF/EF31_regionalized-for-ecoinvent-v39.ff0965b0f9793fbd2a351c9155946122.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

        with as_file(
            files("regioinvent").joinpath(
                "data/ReCiPe/ReCiPe_regionalized-for-ecoinvent-v39.d03db1f1699b4f0b4d72626e52a40647.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

    if lcia_method == "IW v2.1" and regio.ecoinvent_version == "3.10":
        regio.logger.info(
            "Importing the fully regionalized version of IMPACT World+ v2.1 for ecoinvent3.10."
        )

        with as_file(
            files("regioinvent").joinpath(
                "data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v310.0fffd5e3daa5f4cf11ef83e49c375827.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

    elif lcia_method == "IW v2.1" and regio.ecoinvent_version == "3.9":
        regio.logger.info(
            "Importing the fully regionalized version of IMPACT World+ v2.1 for ecoinvent3.9."
        )

        with as_file(
            files("regioinvent").joinpath(
                "data/IW/impact_world_plus_21_regionalized-for-ecoinvent-v39.af770e84bfd0f4365d509c026796639a.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

    elif lcia_method == "EF v3.1" and regio.ecoinvent_version == "3.10":
        regio.logger.info("Importing the fully regionalized version of EF v3.1 for ecoinvent 3.10.")

        with as_file(
            files("regioinvent").joinpath(
                "data/EF/EF31_regionalized-for-ecoinvent-v310.87ec66ed7e5775d0132d1129fb5caf03.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

    elif lcia_method == "EF v3.1" and regio.ecoinvent_version == "3.9":
        regio.logger.info("Importing the fully regionalized version of EF v3.1 for ecoinvent 3.9.")

        with as_file(
            files("regioinvent").joinpath(
                "data/EF/EF31_regionalized-for-ecoinvent-v39.ff0965b0f9793fbd2a351c9155946122.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

    elif lcia_method == "ReCiPe 2016 v1.03 (H)" and regio.ecoinvent_version == "3.10":
        regio.logger.info(
            "Importing the fully regionalized version of ReCiPe 2016 v1.03 (H) for ecoinvent 3.10."
        )

        with as_file(
            files("regioinvent").joinpath(
                "data/ReCiPe/ReCiPe_regionalized-for-ecoinvent-v310.dd7e66b1994d898394e3acfbed8eef83.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)

    elif lcia_method == "ReCiPe 2016 v1.03 (H)" and regio.ecoinvent_version == "3.9":
        regio.logger.info(
            "Importing the fully regionalized version of ReCiPe 2016 v1.03 (H) for ecoinvent 3.9."
        )

        with as_file(
            files("regioinvent").joinpath(
                "data/ReCiPe/ReCiPe_regionalized-for-ecoinvent-v39.d03db1f1699b4f0b4d72626e52a40647.bw2package"
            )
        ) as file_path:
            bw2.BW2Package.import_file(file_path)
