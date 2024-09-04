#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      r.in.wcs.worker
# AUTHOR(S):   Anika Weinmann, Lina Krisztian
#
# PURPOSE:     Worker addon of r.in.wcs which imports GetCoverage from a WCS
#              server via requests
# COPYRIGHT:   (C) 2023-2024 by Anika Weinmann, mundialis GmbH & Co. KG and the
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
# % description: Worker addon of r.in.wcs which imports GetCoverage from a WCS server via requests.
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
# %end

# %option G_OPT_V_INPUT
# % key: area
# %end

# %option
# % key: coverageid
# % type: string
# % description: Coverage
# % required: yes
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

# %option
# % key: new_mapset
# % type: string
# % required: yes
# % multiple: no
# % label: Name of new mapset for parallel processing
# %end

# %option
# % key: subset_type
# % type: string
# % required: yes
# % multiple: no
# % label: Type for subset settings e.g. "Lat Long"
# %end

# %option
# % key: num_retry
# % type: int
# % required: no
# % answer: 0
# % multiple: no
# % label: Number of download retries
# %end

# %rules
# % collective: username,password
# %end

import atexit
import os
from time import sleep
import sys

from urllib.request import urlretrieve
from urllib.error import URLError

from grass.script import core as grass
from grass.pygrass.utils import get_lib_path

try:
    from grass_gis_helpers.mapset import switch_to_new_mapset
    from grass_gis_helpers.validation import get_gdalinfo_returncodes
except ImportError:
    grass.fatal(
        _(
            "Please check if the python library grass_gis_helpers is "
            "installed or install it with: <pip install grass-gis-helpers>"
        )
    )

# initialize global vars
RM_FILES = []


def cleanup():
    """Cleanup function"""
    for file in RM_FILES:
        if os.path.isfile(file):
            grass.try_remove(file)


def main():
    """Main function of r.in.wcs"""
    global RM_FILES

    path = get_lib_path(modname="r.in.wcs", libname="r_in_wcs_lib")
    if path is None:
        grass.fatal("Unable to find the r.in.wcs library directory.")
    sys.path.append(path)
    try:
        # pylint: disable=import-outside-toplevel,no-name-in-module
        from r_in_wcs_lib import (
            set_url,
            set_user_pw,
        )
    except ImportError:
        grass.fatal("analyse_trees_lib missing.")

    res = grass.region()["nsres"]

    # switching mapset
    new_mapset = options["new_mapset"]
    gisrc, newgisrc, old_mapset = switch_to_new_mapset(new_mapset)

    wcs_url = options["url"]
    coverageid = options["coverageid"]
    area = f"{options['area']}@{old_mapset}"
    num_retry_max = options["num_retry"]

    # setting region to area
    grass.run_command("g.region", vector=area, res=res)

    url = set_url(
        wcs_url,
        coverageid,
        out=options["output"],
        axis=options["subset_type"],
    )[0]
    set_user_pw(url, options["username"], options["password"])

    grass.message(_("Retrieving data..."))
    tif = grass.tempfile()
    os.remove(tif)
    tif = tif.replace(".0", ".tif")
    RM_FILES.append(tif)

    num_retry_no_connection = 0
    while num_retry_no_connection <= num_retry_max:
        try:
            num_retry_unstable_connection = 0
            urlretrieve(url, tif)
            gdalinfo_err, gdalinfo_returncode = get_gdalinfo_returncodes(tif)
            if (
                gdalinfo_returncode != 0
                or ("TIFFReadEncodedStrip" in gdalinfo_err)
                or ("TIFFReadEncodedTile" in gdalinfo_err)
            ):
                if num_retry_unstable_connection == num_retry_max:
                    grass.fatal(
                        _(
                            "Failed to download tif after "
                            f"{num_retry_max} retries."
                        )
                    )
                grass.warning(
                    _(
                        f"Broken tif downloaded, with error {gdalinfo_err}."
                        " Try to re-download. Retry "
                        f"{num_retry_unstable_connection}/{num_retry_max} ..."
                    )
                )
                sleep(5)
                os.remove(tif)
                urlretrieve(url, tif)
                gdalinfo_err, gdalinfo_returncode = get_gdalinfo_returncodes(
                    tif
                )
                num_retry_unstable_connection += 1
            else:
                break
        except URLError as e:
            if num_retry_no_connection == num_retry_max:
                grass.fatal(
                    _(
                        f"Failed to reach the server.\nURL: {url} "
                        f"after {num_retry_max} retries."
                    )
                )
            grass.warning(
                _(
                    f"Failed to reach the server.\nURL: {url}. With Error {e}. "
                    "Retry {num_retry_no_connection}/{num_retry_max} ..."
                )
            )
            sleep(5)
            num_retry_no_connection += 1

    grass.run_command("r.import", input=tif, output=options["output"])
    grass.message(
        _(f"WCS Coverage {coverageid} is impored as {options['output']}")
    )
    # set GISRC to original gisrc and delete newgisrc
    os.environ["GISRC"] = gisrc
    grass.try_remove(newgisrc)
    grass.message(
        _(f"Calculation of r.in.wcs.worker for subset {options['area']} DONE")
    )


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
