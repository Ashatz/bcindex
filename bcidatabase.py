#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Andrew Shatz
#
# Created:     24/10/2014
# Copyright:   (c) Andrew Shatz 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import os, sys, shutil
import sqlite3

class bcisqlitedb():

    def __init__(self, database):
        d = os.path.dirname(database)
        if d == '':
            database = os.path.join(os.getcwd, database)
        test = os.path.exists(database)
        self.DbPath = database
        if test == False:
            self.createBCIDatabase(self.DbPath)


    def createBCIDatabase(self, database):

        #Create connection and cursor object
        conn = sqlite3.connect(database)
        cur = conn.cursor()

        ###Create Database Schema###
        print(" * * * Creating database schema... * * * ")

        #Create table tBCIProject
        sql = """CREATE TABLE tBCIProject (
                    BCIProjectSID INT PRIMARY KEY ASC,
                    ProjectName VARCHAR(100),
                    Species VARCHAR(100),
                    RasterDatasetPath VARCHAR(200),
                    ValidDatasetPath VARCHAR(200),
                    MaskPath VARCHAR(200))"""
        cur.executescript(sql)

        #Create table tTileSet
        sql = """CREATE TABLE tTileSet (
                    TileSetSID INT PRIMARY KEY ASC,
                    BCIProjectSID INT,
                    XOffset INT,
                    YOffset INT,
                    XCoordinate FLOAT,
                    YCoordinate FLOAT,
                    Columns INT,
                    Rows INT,
                    FOREIGN KEY(BCIProjectSID) REFERENCES tBCIProject(BCIProjectSID))"""
        cur.executescript(sql)

        #Create table tMaskDataset
        sql = """CREATE TABLE tMaskDataset (
                    MaskDatasetSID INT PRIMARY KEY ASC,
                    BCIProjectSID INT,
                    DatasetName VARCHAR(100),
                    DatasetType VARCHAR(50),
                    Projection VARCHAR(100),
                    FOREIGN KEY(BCIProjectSID) REFERENCES tBCIProject(BCIProjectSID))"""
        cur.executescript(sql)

        #Create table tRasterDataset
        sql = """CREATE TABLE tRasterDataset (
                    RasterDatasetSID INT PRIMARY KEY ASC,
                    PartitionSID INT
                    DatasetName VARCHAR(100),
                    DatasetType VARCHAR(50),
                    XOrigin FLOAT,
                    YOrigin FLOAT,
                    Columns INT,
                    Rows INT,
                    XResolution FLOAT,
                    YResolution FLOAT,
                    FOREIGN KEY(PartitionSID) REFERENCES tPartition(PartitionSID))"""
        cur.executescript(sql)

        #Create table tValidDataset
        sql = """CREATE TABLE tValidDataset (
                    ValidDatasetSID INT PRIMARY KEY ASC,
                    PartitionSID INT,
                    DatasetName VARCHAR(100),
                    DatasetType VARCHAR(50),
                    SpeciesName VARCHAR(100),
                    LocationTotal INT,
                    FOREIGN KEY(PartitionSID) REFERENCES tPartition(PartitionSID))"""
        cur.executescript(sql)

        #Create table tPartition
        sql = """CREATE TABLE tPartition (
                    PartitionSID INT PRIMARY KEY ASC,
                    BCIProjectSID INT,
                    RasterDatasetSID INT,
                    ValidDatasetSID INT,
                    FOREIGN KEY(BCIProjectSID) REFERENCES tBCIProject(BCIProjectSID),
                    FOREIGN KEY(RasterDatasetSID) REFERENCES tRasterDataset(RasterDatasetSID),
                    FOREIGN KEY(ValidDatasetSID) REFERENCES tValidDataset(ValidDatasetSID))"""
        cur.executescript(sql)


        #Create tRasterTile
        sql = """CREATE TABLE tRasterTile (
                    RasterTileSID INT PRIMARY KEY ASC,
                    RasterDatasetSID INT,
                    TileSetSID INT,
                    GridData BLOB,
                    FOREIGN KEY(RasterDatasetSID) REFERENCES tRasterDataset(RasterDatasetSID),
                    FOREIGN KEY(TileSetSID) REFERENCES tTileSet(TileSetSID))"""
        cur.executescript(sql)

        #Create table tValidLocation
        sql = """CREATE TABLE tValidLocation (
                    ValidLocationSID SID INT PRIMARY KEY ASC,
                    ValidDatasetSID INT,
                    TileSetSID INT,
                    XCoord FLOAT,
                    YCoord FLOAT,
                    XOff INT,
                    YOff INT,
                    FOREIGN KEY(ValidDatasetSID) REFERENCES tValidDataset(ValidDatasetSID),
                    FOREIGN KEY(TileSetSID) REFERENCES tTileSet(TileSetSID))"""
        cur.executescript(sql)

        #Create table tMaskTile
        sql = """CREATE TABLE tMaskTile (
                    MaskTileSID INT PRIMARY KEY ASC,
                    MaskDatasetSID INT,
                    TileSetSID INT,
                    GridData BLOB,
                    FOREIGN KEY(MaskDatasetSID) REFERENCES tMaskDataset(MaskDatasetSID),
                    FOREIGN KEY(TileSetSID) REFERENCES tTileSet(TileSetSID))"""
        cur.executescript(sql)

        #Create table tSIDUtil
        sql = """CREATE TABLE tSIDUtil (
                    TableSID INT PRIMARY KEY ASC,
                    TableName VARCHAR(50),
                    MaxSID INT)"""
        cur.executescript(sql)

        #Create table tempRasterTile
        print(" * * * Creating table tempRasterTile... * * * ")
        sql = """CREATE TABLE tempRasterTile (
                    RecordID INT,
                    tempGridData BLOB,
                    tempTopXOff INT,
                    tempTopYOff INT,
                    tempTileColumns INT,
                    tempTileRows INT)"""
        cur.executescript(sql)

        #Create table tempLocation
        print(" * * * Creating table tempLocation... * * * ")
        sql = """CREATE TABLE tempLocation (
                    RecordId INT,
                    tempXCoord FLOAT,
                    tempYCoord FLOAT,
                    tempXOff INT,
                    tempYOff INT)"""
        cur.executescript(sql)

        #Load tSIDUtil table
        print(" * * * Loading table tSIDUtil... * * * ")
        rows = (
            (1, 'tBCIProject', 0),
            (2, 'tTileSet', 0),
            (3, 'tRasterDataset', 0),
            (4, 'tValidDataset', 0),
            (5, 'tMaskDataset', 0),
            (6, 'tPartition', 0),
            (7, 'tRasterTile', 0),
            (8, 'tValidLocation', 0),
            (9, 'tMaskTile', 0)
        )
        cur.executemany("INSERT INTO tSIDUtil VALUES (?,?,?)", rows)

        #All done!
        conn.commit()
        conn.close()
        print(" * * * Database schema loaded. * * * ")

    #Complete
    def pSIDUtil(self, table_name, ammount):
        conn = sqlite3.connect(self.DbPath)
        cur = conn.cursor()
        cur.execute("SELECT MaxSID FROM tSIDUtil WHERE TableName = '" + table_name + "'")
        currentSID = [item[0] for item in cur.fetchall()]
        currentSID = currentSID[0]
        nextSID = ammount + currentSID
        cur.execute("UPDATE tSIDUtil SET MaxSID = " + str(nextSID) + " WHERE TableName = '" + table_name + "'")
        conn.close()
        return nextSID

    def pLoadBCIProject(self, BCIProject):
        print(" * * * Loading BCI Project into database... * * * ")
        nextSID = self.pSIDUtil("tBCIProject", 1)
        conn = sqlite3.connect(self.DbPath)
        cur = conn.cursor()
        data = [(nextSID, BCIProject.ProjectName, BCIProject.Species,
                     BCIProject.RasterDatasetPath, BCIProject.ValidDatasetPath,
                     BCIProject.MaskDatasetPath)]
        cur.executemany("INSERT INTO tBCIProject VALUES (?,?,?,?,?,?)", data)
        conn.commit()
        print(" * * * Project loaded successfully. * * * ")
        """
        except:
            conn.rollback()
            print(" * * * Project failed to load! * * * ")
        """

    #needs update
    def pLoadRasterDataset(self, data):
        print(" * * * Loading data to tRasterDataset... * * * ")
        nextSID = self.pSIDUtil("tRasterDataset", len(data))
        data = self.UpdateNextSID(data, nextSID)
        conn = sqlite3.connect(self.DbPath)
        cur = conn.cursor()
        cur.executemany("INSERT INTO tRasterDataset VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", data)
        conn.commit()
        conn.close()
        print(" * * * Loading complete! * * * ")

    #needs update
    def pLoadTileSet(self, data):
        print(" * * * Loading data to tTileSet... * * * ")
        nextSID = self.pSIDUtil("tTileSet", len(data))
        #Update SID values for for incoming data
        data = self.UpdateNextSID(data, nextSID)
        #Update Foreign key values in original dataset
        value = data[0][1]
        conn = sqlite3.connect(self.DbPath)
        cur = conn.cursor()
        cur.execute("SELECT RasterDatasetSID FROM tRasterDataset WHERE DatasetName = '" + value + "'")
        RdSID = cur.fetchall()
        data = self.UpdateForeignKey("tRasterDataset", "DatasetName", value, data, 1)
        cur.executemany("INSERT INTO tTileSet VALUES (?,?,?,?,?)", data)
        conn.close()
        print(" * * * Loading complete")

    #Needs update
    def pLoadRasterTile(self, RasterDataset, TileSet, data):
        print(" * * * Loading data to tRasterTile... * * * ")
        #Load data into temp table
        conn = sqlite3.connect(self.DbPath)
        cur = conn.cursor()
        cur.execute("DELETE FROM tempRasterTile")
        cur.executemany("INSERT INTO tempRasterTile VALUES (?,?,?,?,?,?)", data)
        conn.commit()
        name = RasterDataset.DatasetName
        #Get nextSID
        nextSID = self.pSIDUtil("tRasterTile", len(data))
        #Get RasterDatasetSID
        cur.execute("SELECT RasterDatasetSID FROM tRasterDataset WHERE DatasetName = '" + name + "'")
        datasetSID = [i[0] for i in cur.fetchall()][0]
        #Get TileSetSID
        cur.execute("SELECT TileSetSID FROM tTileSet JOIN tRasterDataset "\
                    "ON tTileSet.RasterDatasetSID = tRasterDataset.RasterDatasetSID "\
                    "WHERE DatasetName = '" + name + "'")
        tilesetSID = [i[0] for i in cur.fetchall()][0]
        sql = "INSERT INTO tRasterTile("\
                    "RasterTileSID, RasterDatasetSID, TileSetSID,"\
                    "GridData, TopXOff, TopYOff, TileColumns, TileRows) "\
                "SELECT "\
                     "RecordID + " + str(nextSID) + " AS RasterTileSID, "\
                     "" + str(datasetSID) + " AS RasterDatasetSID, "\
                     "" + str(tilesetSID) + " AS TileSetSID, "\
                     "tempGridData AS GridData, "\
                     "tempTopXOff AS TopXOff, "\
                     "tempTopYOff AS TopYOff, "\
                     "tempTileColumns AS TileColumns, "\
                     "tempTileRows AS TileRows "\
                "FROM tempRasterTile"
        cur.executescript(sql)

    #Works
    def UpdateNextSID(self, data, nextSID):
        output = []
        for d in data:
            index = data.index(d)
            temp = list(d)
            temp[0] = nextSID + index
            output.append(tuple(temp))
        return output

    #Test for usefulness...
    def UpdateForeignKey(self, orig_table, where_field, value, data, index):
        #Find the SID field
        SIDField = orig_table[1:] + "SID"
        #Get the Foreign key value from the original table
        conn = sqlite3.connect(self.DbPath)
        cur = conn.cursor()
        cur.execute("SELECT " + SIDField + " FROM " + orig_table + " WHERE " + where_field + " = '" + value + "'")
        SIDValue =  [i[0] for i in cur.fetchall()][0]
        output = []
        for d in data:
            temp = list(d)
            temp[index] = SIDValue
            output.append(tuple(temp))
        return output