#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      r.in.wcs
# AUTHOR(S):   Anika Weinmann
#
# PURPOSE:     Imports GetCoverage from a WCS server via requests
# COPYRIGHT:   (C) 2023 by Anika Weinmann, mundialis GmbH & Co. KG and the
#              GRASS Development Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#############################################################################

# %module
# % description: Imports GetCoverage from a WCS server via requests.
# % keyword: raster
# % keyword: import
# % keyword: OGC web services
# % keyword: OCC WCS
# %end

# %option
# % key: url
# % type: string
# % description: Service URL (typically http://.../mapserv? )
# % required: yes
# %end

# %option G_OPT_V_OUTPUT
# % required: no
# %end

# %option
# % key: coverageid
# % type: string
# % description: Coverage
# % required: no
# %end

# %option
# % key: username
# % type: string
# % required: no
# % multiple: no
# % label: Username or file with username or environment variable name with username
# %end

# %option
# % key: password
# % type: string
# % required: no
# % multiple: no
# % label: Password or file with password or environment variable name with password
# %end

# %option G_OPT_M_NPROCS
# % description: Number of cores for multiprocessing, -2 is the number of available cores - 1
# % answer: -2
# %end

# %flag
# % key: c
# % description: GetCapabilities of WCS
# %end

# %flag
# % key: d
# % description: DescribeCoverage of WCS Coverage
# %end

# %rules
# % exclusive: output,-c,-d
# % required: output,-c,-d
# % collective: username,password
# % requires: coverageid,-d,output
# %end

import atexit
import os
import sys

from grass.script import core as grass
from grass.pygrass.utils import get_lib_path
from grass.script.utils import try_rmdir

from grass_gis_helpers.general import set_nprocs, rm_vects
from grass_gis_helpers.tiling import create_grid
from grass_gis_helpers.parallel import (
    patching_raster_results,
    run_module_parallel,
)


# initialize global vars
LOCATION_PATH = None
MAPSET_NAMES = []
NPROCS = None
RM_VECTORS = []


def cleanup():
    """Cleanup function"""
    rm_vects(RM_VECTORS)
    # Delete temp_mapsets
    for new_mapset in MAPSET_NAMES:
        try_rmdir(os.path.join(LOCATION_PATH, new_mapset))


def main():
    """Main function of r.in.wcs"""
    global MAPSET_NAMES, LOCATION_PATH, NPROCS, RM_VECTORS

    path = get_lib_path(modname="r.in.wcs", libname="r_in_wcs_lib")
    if path is None:
        grass.fatal("Unable to find the r.in.wcs library directory.")
    sys.path.append(path)
    try:
        # pylint: disable=import-outside-toplevel,no-name-in-module
        from r_in_wcs_lib import (
            set_url,
            get_xml_data,
        )
    except ImportError:
        grass.fatal("analyse_trees_lib missing.")

    wcs_url = options["url"]
    coverageid = options["coverageid"]
    NPROCS = set_nprocs(options["nprocs"])

    if flags["c"] or flags["d"]:
        url, msg = set_url(wcs_url, coverageid)
        pretty_xml = get_xml_data(
            url, options["username"], options["password"]
        )
        print(f"{msg}:\n{pretty_xml}")
    elif options["output"]:
        tmp_id = grass.tempname(12)
        tiles_list = create_grid(1000, "wcs_grid", tmp_id)
        RM_VECTORS.extend(tiles_list)

        module_kwargs = {
            "url": wcs_url,
            "output": options["output"],
            "coverageid": coverageid,
            "username": options["username"],
            "password": options["password"],
        }
        MAPSET_NAMES, LOCATION_PATH = run_module_parallel(
            "r.in.wcs.worker",
            module_kwargs,
            tiles_list,
            NPROCS,
            tmp_id,
            # parallel=False,
        )
        if len(MAPSET_NAMES) != len(tiles_list):
            return 1
        patching_raster_results(MAPSET_NAMES, options["output"])
        grass.message(_(f"Ouput raster map {options['output']} created."))


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
