#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      r.in.wcs tests
# AUTHOR(S):   Anika Weinmann
#
# PURPOSE:     Tests r.in.wcs
# COPYRIGHT:   (C) 2023 by mundialis GmbH & Co. KG and the GRASS Development
#              Team
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
#############################################################################

import os

from grass.gunittest.case import TestCase
from grass.gunittest.main import test
from grass.gunittest.gmodules import SimpleModule
import grass.script as grass


class TestRInWcs(TestCase):
    """Test class for r.in.wcs"""

    pid = os.getpid()
    region = f"r_in_wcs_orig_region_{pid}"
    out = f"r_in_wcs_test_output_{pid}"
    # WCS and coverageId
    url = "https://geoserver.mundialis.de/geoserver/global/ows?"
    coverageid = "global__worldpop_2020_1km_aggregated_UNadj"
    num_coverage_ids = 11
    coverage_name = "Worldpop 2020 1km aggregated UNadj Pop. density"
    north = 186650
    south = 185425
    west = 172622
    east = 174012
    num_data = None

    @classmethod
    # pylint: disable=invalid-name
    def setUpClass(cls):
        """Ensures expected computational region"""
        cls.num_data = len(
            grass.parse_command("g.list", type="all", mapset=".")
        )
        # set region
        cls.runModule("g.region", save=cls.region)
        cls.runModule(
            "g.region",
            n=cls.north,
            s=cls.south,
            w=cls.west,
            e=cls.east,
            res=1,
        )

    @classmethod
    # pylint: disable=invalid-name
    def tearDownClass(cls):
        """Remove the temporary region and generated data"""
        cls.runModule("g.region", region=cls.region)
        cls.runModule("g.remove", type="region", name=cls.region, flags="f")
        # check number of data in mapset
        num_data = len(grass.parse_command("g.list", type="all", mapset="."))
        if num_data != cls.num_data:
            cls.fail(cls, "Test or addon does not cleaned up correctly.")

    # pylint: disable=invalid-name
    def tearDown(self):
        """Remove the outputs created
        This is executed after each test run.
        """
        if grass.find_file(name=self.out, element="raster")["file"]:
            self.runModule(
                "g.remove",
                type="raster",
                name=f"{self.out}",
                flags="f",
            )

    def test_getcapabilities(self):
        """
        Tests GetCapabilities
        """
        print("\nTest GetCapabilities ...")
        r_check = SimpleModule(
            "r.in.wcs",
            url=self.url,
            flags="c",
        )
        self.assertModule(r_check, "GetCapabilities fails.")
        stdout = r_check.outputs.stdout
        self.assertIn(
            self.coverageid,
            stdout,
            "Coverage not in xml GetCapabilities stdout",
        )
        print("Test GetCapabilities successfully finished.\n")

    def test_list_coverageids(self):
        """
        Tests list coverage ids
        """
        print("\nTest list coverage ids ...")
        r_check = SimpleModule(
            "r.in.wcs",
            url=self.url,
            flags="l",
        )
        self.assertModule(r_check, "List coverage ids fails.")
        stdout = r_check.outputs.stdout
        self.assertIn(
            self.coverageid,
            stdout,
            "Coverage not in list of coverage id",
        )
        self.assertEqual(
            len(stdout.split("\n")),
            self.num_coverage_ids,
            f"Length of the coverage id list not {self.num_coverage_ids}.",
        )
        print("Test list coverage ids successfully finished.\n")

    def test_describe_coverage(self):
        """
        Tests DescribeCoverage
        """
        print("\nTest DescribeCoverage ...")
        r_check = SimpleModule(
            "r.in.wcs",
            url=self.url,
            flags="d",
            coverageid=self.coverageid,
        )
        self.assertModule(r_check, "DescribeCoverage fails.")
        stdout = r_check.outputs.stdout
        self.assertIn(
            self.coverageid,
            stdout,
            "Coverage id not in xml DescribeCoverage stdout",
        )
        self.assertIn(
            self.coverage_name,
            stdout,
            "Coverage name not in xml DescribeCoverage stdout",
        )
        print("Test DescribeCoverage successfully finished.\n")

    def test_data_import(self):
        """
        Tests data import
        """
        print("\nTest data import ...")
        r_check = SimpleModule(
            "r.in.wcs",
            url=self.url,
            coverageid=self.coverageid,
            output=self.out,
            tile_size=10000,
        )
        self.assertModule(r_check, "data import fails.")
        self.assertRasterExists(self.out)
        self.assertRasterMinMax(
            self.out, 32.67583, 184.805, "Raster range wrong"
        )
        # check extent
        info = grass.parse_command("r.info", map=self.out, flags="g")
        self.assertTrue(
            (
                self.north > float(info["north"])
                and float(info["south"]) > self.south
                and self.east > float(info["east"])
                and float(info["west"]) > self.west
            ),
            "Extent of output raster wrong",
        )
        print("Test data import successfully finished.\n")


if __name__ == "__main__":
    test()
