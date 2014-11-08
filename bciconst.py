#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Andrew Shatz
#
# Created:     06/11/2014
# Copyright:   (c) Andrew Shatz 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import os, sys
from osgeo import gdal
from osgeo.gdalconst import *


#Declare default variables
BASE_PATH = os.path.dirname(__file__)
DB_BPATH = os.path.join(BASE_PATH, "Databases")


#Declare file extension filters
RST_EXT = [".rst", ".tif"]
VCT_EXT = [".shp"]

#Dictionary of raster data types...to be continued.
#Add entries according to workable datatypes for GDAL
RST_TYPES = {".rst":"IDRISI Raster A1", ".tif":"GeoTiff"}
VCT_TYPES = {".shp":"ESRI Shapefile", ".vct":"IDRISI Vector"}

#Establish default tile size
TILE_SIZE = (100,100)

#Declare Error Constants
FTYPE_ERROR1 = -10
FTYPE_ERROR2 = -11
IO_ERROR1 = -100