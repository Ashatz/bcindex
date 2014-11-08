#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Andrew Shatz
#
# Created:     28/10/2014
# Copyright:   (c) Andrew Shatz 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import os, sys, shutil
from osgeo import gdal
from osgeo.gdalconst import *
import numpy as np
from sqlite3 import connect, Binary
from struct import unpack
from bciconst import *

def calculateOffset(coord, resolution, origin):
    return int((coord - origin)/resolution)

def calculateOriginCoord(offset, resolution, origin):
    pass

class BCIProject():
    def __init__(self, db_name, project_name, species, sdm_dataset, valid_dataset, mask_bool, mask_file):
        self._DB_PATH = os.path.join(DB_BPATH, db_name)
        self.ProjectName = project_name
        self.Species = species
        self.RasterDatasetPath = sdm_dataset
        self.ValidDatasetPath = valid_dataset
        if mask_bool == True:
            self.MaskDatasetPath = mask_file
        elif mask_bool == False:
            self.MaskDatasetPath = self._findMaskPath(self._DB_PATH, mask_file)

    def _findMaskPath(self, db_path, mask_file):
        conn = connect(db_path)
        cur = conn.cursor()
        sql = """SELECT MaskDatasetPath FROM tBCIProject JOIN tMaskDataset ON
                    tBCIProject.BCIProjectSID = tMaskDataset.BCIProjectSID
                    WHERE DatasetName = """ +  mask_file
        cur.executescript(sql)
        row = cur.fetchone()
        return row[0]




class RasterDataset():

    def __init__(self, data_type, dataset, part_number):
        if data_type in data_types.values():
            rstds = gdal.Open(dataset, GA_ReadOnly)
            b = os.path.basename(dataset) #get basename
            print os.getcwd()
            print os.path.dirname(dataset)
            if os.path.dirname(dataset) == '':
                self.DatasetOrigin = os.getcwd()
            else:
                self.DatasetOrigin = os.path.dirname(dataset)
            s = os.path.splitext(b) #split extension
            self.DatasetName = s[0]
            self.DatasetType = data_types[s[1]]
            self.PartNumber = part_number
            gt = rstds.GetGeoTransform()
            self.TopX = gt[0]
            self.TopY = gt[3]
            self.XRes = gt[1]
            self.YRes = gt[5]
            self.Columns = rstds.RasterXSize
            self.Rows = rstds.RasterYSize
            self.Projection = rstds.GetProjection()

    def toSqliteList(self):
        output = [(0, self.DatasetOrigin, self.DatasetName, self.DatasetType,
                   self.PartNumber, self.TopX, self.TopY, self.Columns,
                   self.Rows, self.XRes, self.YRes, self.Projection)]
        return output

class TileSet():

    def __init__(self, Columns, Rows, MaxXSize, MaxYSize):
        self.DatasetName = RasterDataset.DatasetName
        self.MaxXSize = MaxXSize
        self.MaxYSize = MaxYSize
        self.Columns = Columns
        self.Rows = Rows
        self.TileNumber = self.getTileNumber()

    def getTileNumber(self):
        xTiles = self.Columns / self.MaxXSize
        if self.Columns % self.MaxXSize > 0:
            xTiles += 1
        yTiles = self.Rows / self.MaxYSize
        if self.Rows % self.MaxYSize > 0:
            yTiles += 1
        return xTiles * yTiles

    def toSqliteList(self):
        output = [(0, self.DatasetName, self.MaxXSize, self.MaxYSize,
                   self.TileNumber)]
        return output
"""
class RasterTile(RasterDataset, TileSet):

    def __init__(self, RasterDataset, TileSet, xOffset, yOffset):
        self.DatasetName = RasterDataset.DatasetName
        self.DatasetType = RasterDataset.DatasetType
        self.DatasetOrigin = RasterDataset.DatasetOrigin
        self.Columns = RasterDataset.Columns
        self.Rows = RasterDataset.Rows
        self.MaxXSize = TileSet.MaxXSize
        self.MaxYSize = TileSet.MaxYSize
        self.TopXOff = xOffset
        self.TopYOff = yOffset
        self.TileColumns = self.CalculateTileDimensions(self.TopXOff, self.Columns, self.MaxXSize)
        self.TileRows = self.CalculateTileDimensions(self.TopYOff, self.Rows, self.MaxYSize)
        ext = retrieveDataTypeExtension(self.DatasetType)
        dsPath = self.ReturnDatasetPath(self.DatasetName, self.DatasetOrigin, ext)
        self.GridData = self.RasterToArray(dsPath, self.TopXOff, self.TopYOff, self.TileColumns, self.TileRows)

    def CalculateTileDimensions(self, offset, cols_rows, max_size):
        if (cols_rows - offset)/max_size > 0:
            return max_size
        elif (cols_rows - offset)/max_size == 0:
            return cols_rows - offset

    def CalculateCoordinates(self, offset, resolution, origin):
        return float(origin + (offset * resolution))

    def CalculateOffset(self, coordinate, resolution, origin):
        return int((coordinate - origin)/resolution)

    def ReturnDatasetPath(self, ds_name, ds_path, ds_type):
        ext = retrieveDataTypeExtension(ds_type)
        return os.path.join(ds_path, ds_name+ds_type)

    def RasterToArray(self, ds_path, x_offset, y_offset, cols, rows):
        #gdal.AllRegister()
        rstds = gdal.Open(ds_path, GA_ReadOnly)
        b = rstds.GetRasterBand(1)
        d = b.ReadAsArray(x_offset, y_offset, cols, rows)
        return d
"""
"""
    def DisplayTileSummary(self):
        print("Raster tile X Offset: %d"%self.TopXOff)
        print("Raster tile Y Offset: %d"%self.TopYOff)
        print("Number of columns: %d"%self.Columns)
        print("Number of rows: %d"%self.Rows)
"""
"""
class LocDataset(RasterTile):

    def __init__(self, species, dataset, partition):
        self.Species = species
        #[self.DatasetOrigin, self.DatasetName, self.DatasetType] = _parseDatasetPath(dataset)
        self.PartNumber = partition
"""
class LocSpecies():
    pass

def retrieveDataTypeExtension(data_type):
    for ext, ftype in data_types.items():
        if data_type == ftype:
             return ext
"""
def formatRasterTileSqliteData(RasterDataset, TileSet):
    #List input parameters
    cols = RasterDataset.Columns
    rows = RasterDataset.Rows
    name = RasterDataset.DatasetName
    XSize = TileSet.MaxXSize
    YSize = TileSet.MaxYSize
    #Create output list
    recordID = 1
    output = []
    for rownum in range(0, rows, YSize):
        for colnum in range(0, cols, XSize):
            rstTile = RasterTile(RasterDataset, TileSet, colnum, rownum)
            byte = rstTile.GridData.tobytes()
            bins = Binary(byte)
            #print(rstTile.TileColumns, rstTile.TileRows) test
            temp = [recordID, bins, rstTile.TopXOff,
                    rstTile.TopYOff, rstTile.TileColumns, rstTile.TileRows]
            output.append(tuple(temp))
            recordID += 1
    return output
"""
"""
    def getDatasetType(self, dataset):
        files = os.listdir(dataset)
        ftypes = []
        for f in files:
            ext = os.path.splitext(f)[-1]
            if ext not in ftypes:
                ftypes.append(ext)
        if len(ftypes) > 1:
            return FTYPE_ERROR1
        elif len(ftypes) == 0:
            return IO_ERROR1
        else:
            ftype = ftypes[0]
            if ftypes[0] in RST_EXT:
                return data_types[ftypes[0]]
            elif ftypes[0] in VCT_EXT:
                return VCT_TYPES[ftypes[0]]
            else:
                return FTYPE_ERROR2
"""
"""
def _parseDatasetPath(dataset):
    if os.path.dirname(dataset) == '':
            DatasetOrigin = os.getcwd()
    else:
        DatasetOrigin = os.path.dirname(dataset)
    b = os.path.basename(dataset)
    s = os.path.splitext(b) #split extension
    DatasetName = s[0]
    DatasetType = data_types[s[1]]
    return DatasetOrigin, DatasetName, DatasetType
"""