## DESCRIPTION

*r.in.wcs* imports GetCoverage from a WCS server via requests.

## EXAMPLE

### Import WCS of global worldpop map

Import WCS of global worldpop map in \`nc_spm_08\` location:

```sh
g.region n=186650 s=185425 w=172622 e=174012 res=1 -p

# get GetCapabilities
r.in.wcs -c url=https://geoserver.mundialis.de/geoserver/global/ows?
# list coverageIds
r.in.wcs -l url=https://geoserver.mundialis.de/geoserver/global/ows?
# get DescribeCoverage
r.in.wcs -d url=https://geoserver.mundialis.de/geoserver/global/ows? coverageid=global__worldpop_2020_1km_aggregated_UNadj
# importing data
r.in.wcs output=worldpop url=https://geoserver.mundialis.de/geoserver/global/ows? coverageid=global__worldpop_2020_1km_aggregated_UNadj tile_size=10000
```

### Import WCS with overlapping granules

```sh
# By default in descending order of given attribute (sort_attr)
r.in.wcs output=... url=... coverageid=... tile_size=... sort_attr=timestamp
# Set ascending order
r.in.wcs output=... url=... coverageid=... tile_size=... sort_attr=timestamp sort_order=A
```

## SEE ALSO

*[r.import](r.import.md)*

## REQUIREMENTS

```sh
pip install grass-gis-helpers
```

## AUTHOR

Anika Weinmann, [mundialis GmbH & Co. KG](https://www.mundialis.de/),
Germany
