#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      Library for r.in.wcs and r.in.wcs.worker
# AUTHOR(S):   Anika Weinmann
#
# PURPOSE:     Library for r.in.wcs and r.in.wcs.worker
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

import os

from urllib.request import urlopen
from urllib.request import build_opener, install_opener
from urllib.request import (
    HTTPPasswordMgrWithDefaultRealm,
    HTTPBasicAuthHandler,
)
from urllib.error import URLError, HTTPError

from bs4 import BeautifulSoup

import grass.script as grass


def set_url(
    wcs_url,
    coverageid=None,
    out=None,
    version="2.0.1",
    axis="N E",
    sortby_attr=None,
    sortby_order="D",
):
    """Function to set the url for service"""
    # WCS - GetCapabilities
    if coverageid is None or coverageid == "":
        url = f"{wcs_url}service=WCS&version={version}&request=GetCapabilities"
        grass.debug(url)
        msg = f"GetCapabilities of {url}"
    # WCS - DescribeCoverage
    elif out is None:
        url = (
            f"{wcs_url}service=WCS&version={version}&request=DescribeCoverage&"
            f"CoverageId={coverageid}"
        )
        grass.debug(url)
        msg = f"DescribeCoverage of {url}"
    else:
        if axis == "E N":
            reg = grass.region()
            reg_ns = [float(reg["n"]), float(reg["s"])]
            reg_ew = [float(reg["e"]), float(reg["w"])]
            subset = (
                f"&subset=N({min(reg_ns)},{max(reg_ns)})"
                f"&subset=E({min(reg_ew)},{max(reg_ew)})"
            )
        elif axis == "Lat Long":
            reg = grass.parse_command("g.region", flags="bg", quiet=True)
            reg_ns = [float(reg["ll_n"]), float(reg["ll_s"])]
            reg_ew = [float(reg["ll_e"]), float(reg["ll_w"])]
            subset = (
                f"&subset=Lat({min(reg_ns)},{max(reg_ns)})"
                f"&subset=Long({min(reg_ew)},{max(reg_ew)})"
            )
        else:
            grass.fatal(_("Subset not yet supported."))

        url = (
            f"{wcs_url}service=WCS&version={version}&request=GetCoverage&"
            f"CoverageId={coverageid}&format=image/tiff{subset}"
        )
        # see also:
        # https://docs.geoserver.org/main/en/user/services/wcs/vendor.html#sortby
        if sortby_attr:
            url += f"&sortBy={sortby_attr} {sortby_order}".replace(" ", "%20")
        grass.debug(url)
        msg = None
    return url, msg


def set_user_pw(url, user_inp, password_inp):
    """Function to get the username and password from option, file or
    environment variable"""
    if user_inp and password_inp:
        grass.message(_("Setting username and password..."))
        if os.path.isfile(user_inp):
            with open(user_inp, encoding="UTF-8") as user_f:
                filecontent = user_f.read()
                user = filecontent.strip()
        elif user_inp in os.environ:
            user = os.environ[user_inp]
        else:
            user = user_inp

        if os.path.isfile(password_inp):
            with open(password_inp, encoding="UTF-8") as pw_f:
                filecontent = pw_f.read()
                password = filecontent.strip()
        elif password_inp in os.environ:
            password = os.environ[password_inp]
        else:
            password = password_inp

        passmgr = HTTPPasswordMgrWithDefaultRealm()
        passmgr.add_password(None, url, user, password)
        authhandler = HTTPBasicAuthHandler(passmgr)
        opener = build_opener(authhandler)
        install_opener(opener)
        grass.message(_("Setting username and password finished"))


def get_xml_data(url, user, password):
    """Function to get the xml data from url"""
    set_user_pw(url, user, password)
    out = ""
    try:
        with urlopen(url) as inf:
            out += inf.read().decode()
    except HTTPError as http_e:
        # GTC WFS request HTTP failure
        grass.fatal(
            _(
                "The server couldn't fulfill the request.\n"
                f"Error code: {http_e.code}"
            )
        )
    except URLError as url_e:
        grass.fatal(_(f"Failed to reach the server.\nReason: {url_e.reason}"))
    xml_out = BeautifulSoup(out, "xml")
    return xml_out.prettify()
