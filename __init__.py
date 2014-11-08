#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Andrew Shatz
#
# Created:     22/10/2014
# Copyright:   (c) Andrew Shatz 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import os, sys, shutil
import csv
from osgeo import gdal
from osgeo.gdalconst import *
from matplotlib import pyplot as plt
import numpy as np
from bciconst import *
from bciobjects import *
from bcidatabase import *
from bcivalid import *

#Declare input variables
db_name = "test3.db"
project_name = "ALB_BCITest"
sdm_path = r"C:\Users\Andrew Shatz\Documents\Scripts\Python\bcindex\Data\Model_Input\GeoTiff\ALB_MAHAL"
valid_path = r"C:\Users\Andrew Shatz\Documents\Scripts\Python\bcindex\Data\Model_Input\GeoTiff\ALB_VALIDATION"
mask_file = r"C:\Users\Andrew Shatz\Documents\Scripts\Python\bcindex\Data\Model_Input\GeoTiff\SA_2_Mask.tif"
species = "Asian_Longhorned_Beetle"
db_name = "test3.db"



#Declare file lists
sdm_files = os.listdir(sdm_path)
valid_files = os.listdir(valid_path)

#Declare database absolute path
db_path = os.path.join(DB_BPATH, db_name)

project = BCIProject(db_name, project_name, species, sdm_path, valid_path, True, mask_file)

bcidb = bcisqlitedb(db_path)
bcidb.pLoadBCIProject(project)




#Create RasterDataset object
gdal.AllRegister()
def LoadRasterDatasets(sdm_path, sdm_files):
    print(" * * * Loading SDM datasets to " + db_name + "... * * * ")
    os.chdir(sdm_path)
    for dataset in sdm_files:
        i = sdm_files.index(dataset) + 1
        rst = RasterDataset("raster", dataset, i)
        rstTile = TileSet(rst, 100, 100)

        #Test connection
        print(" * * * Establishing database connection... * * * ")

        bcidb = bcisqlitedb(DB_ABSPATH)
        #Test raster dataset loading

        rst_sql = rst.toSqliteList()
        bcidb.pLoadRasterDataset(rst_sql)
        #Test tile set loading
        tile_sql = rstTile.toSqliteList()
        bcidb.pLoadTileSet(tile_sql)
        #Test raster tile loading
        data_sql = formatRasterTileSqliteData(rst, rstTile)
        #print datasql #Test
        bcidb.pLoadRasterTile(rst, rstTile, data_sql)

"""
###This section is meant to test the raster loading procedure for the validation datasets
os.chdir(valid_path)
valid_file = valid_files[0]

valid_ds = LocDataset("Asian Longhorned Beetle", valid_file, 1)
"""



