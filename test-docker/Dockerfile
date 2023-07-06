FROM osgeo/grass-gis:stable-alpine

ARG NC_TEST_DATA=""

WORKDIR /src
COPY . /src
COPY test-docker/test.sh /src
RUN apk add gcc make
RUN test -e requirements.txt && pip3 install -r requirements.txt || echo "No requirements.txt"

# run tests if NC_TEST_DATA is set to NC with downloaded NC test loaction otherwise in empty location
RUN grass -c epsg:3358 /grassdb/nc_spm_empty --exec bash test.sh ${NC_TEST_DATA}
