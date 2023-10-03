from hec.server import RmiAppImpl
from hec.io import Identifier
from hec.rss.model import SimulationExtractModel
from rma.util import RMAIO
from hec.script import Constants

from hec.heclib.dss import HecDss
from hec.io import TimeSeriesContainer
from hec.heclib.util import HecTime
from hec.hecmath import TimeSeriesMath
from hec.heclib.dss import HecDSSFileDataManager
from hec.heclib.dss import HecDSSFileAccess
from hec.ensemble import Ensemble, EnsembleTimeSeries
from hec import SqliteDatabase, VersionIdentifier, RecordIdentifier
from org.sqlite import JDBC
from java.time import Duration, ZonedDateTime, ZoneId
from java.lang import System
from jarray import array
import logging
import os
import sys
import shutil
import datetime

def configureResSim(watershedWkspFile, simName, altName):

    LogLevel = 1 # lower numbers are less logging
    HecDSSFileAccess.setMessageLevel(LogLevel)
    rmiApp = RmiAppImpl.getApp()
    workspaceFile = watershedWkspFile.replace(os.sep, "/")
    assert os.path.isfile(workspaceFile), "####SCRIPT### - Watershed file does exist"

    id = Identifier(workspaceFile)
    user = System.getProperty("user.name")

    rmiWksp = rmiApp.openWorkspace(user, id)
    assert rmiWksp is not None, "ERROR: Failed to open Watershed: %s" %(workspaceFile)

    rssWksp = rmiWksp.getChildWorkspace("rss")

    wtrshdPath= rssWksp.getWorkspacePath()
    simulationPath = wtrshdPath+"/rss/"+simName+".simperiod"
    assert os.path.isfile(simulationPath), "####SCRIPT### - Simulation's simperiod file does exist"

    simId = Identifier(simulationPath)
    simMgr = rssWksp.getManager("hec.model.SimulationPeriod", simId)
    assert simMgr is not None, "ERROR: Failed to getManager for simulation %s" %(simName)

    simMgr.loadWorkspace(None,wtrshdPath)

    simRun = simMgr.getSimulationRun(altName)
    assert simRun is not None, "ERROR: Failed to find SimulationRun: %s " %(altName)

    simRun.getRssAlt().setLogLevel(LogLevel)		#log level controls how much messaging is sent to the console and log
    simMgr.setComputeAll(Constants.TRUE)


    return simMgr, simRun, rmiWksp, user

def myLogger(name, path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(path, 'a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def archiveSimulationResults(aepList, pattern, patternName, resultsDir, simulationDssDir, n):

    bParts = ['FOLSOM-POOL','FOLSOM-POOL','FOLSOM-POOL','FOLSOM-POOL',
              'FOLSOM-CONSERVATION','FOLSOM-CONSERVATION',
              'FOLSOM-FLOOD CONTROL','FOLSOM-FLOOD CONTROL',
              'FOLSOM-SURCHARGE','FOLSOM-SURCHARGE',
              'FOLSOM-7 ESRD SEAL 8 GATES - 120-MIN ISE SLOPE',
              'FOLSOM-EMERGENCY SPILLWAY',
              'PREDICT_PEAK_STORAGE_115',
              'PREDICT_PEAK_STORAGE_130',
              'PREDICT_PEAK_STORAGE_145',
              'PREDICT_PEAK_STORAGE_160']
    cParts = ['FLOW-IN','FLOW-OUT','ELEV','STOR',
              'ELEV-ZONE','STOR-ZONE',
              'ELEV-ZONE','STOR-ZONE',
              'ELEV-ZONE','STOR-ZONE',
              'FLOW-MIN',
              'FLOW',
              'MAX_STOR_115',
              'MAX_STOR_130',
              'MAX_STOR_145',
              'MAX_STOR_160']

    simulationDssFile = r"%s\simulation.dss" %(simulationDssDir)
    resultsDssFile = r"%s\RV-%s_results.dss" %(resultsDir, patternName)

    lookupSimulationDParts = {
        '1986': {
           datetime.datetime(1986,2,1): '01FEB1986'
        },
        '1997': {
            datetime.datetime(1996,12,1): '01DEC1996',
            datetime.datetime(1997,1,1): '01JAN1997'
        }
    }

    simulationDParts = lookupSimulationDParts[pattern]
    pathNames = []
    for bpart, cpart in zip(bParts,cParts):
        for aep in aepList:

            for dpartKey in sorted(simulationDParts.keys()):
                dpart = simulationDParts[dpartKey]
                pathName = "//%s/%s/%s/1HOUR/C:000%s|RV-%s---0/" % (bpart, cpart,dpart, aep, patternName)
                pathNames.append(pathName)

    writeResultsToFile(pathNames, simulationDssFile, resultsDssFile,n )

def writeResultsToFile(pathNames, simulationDssFile, resultsDssFile,n):

    nStr = str(n).zfill(3)
    fid = HecDss.open(simulationDssFile)
    for pathName in pathNames:

        if fid.recordExists(pathName):
            tsc = fid.get(pathName)
            if tsc.values is not None:
                parts = tsc.fullName.split('/')
                parts[6] = parts[6][:-3] + nStr
                newPathName = '/'.join(parts)

                newTs = TimeSeriesContainer()
                newTs.version = newPathName.split('/')[-2]
                newTs.fullName = newPathName
                newTs.timeGranularitySeconds = tsc.timeGranularitySeconds
                newTs.type = tsc.type
                newTs.units = tsc.units
                newTs.interval = tsc.interval
                newTs.numberValues = tsc.numberValues
                newTs.times = tsc.times
                newTs.values = tsc.values

                results = HecDss.open(resultsDssFile)
                results.put(newTs)
                results.done()
        else:
            print 'Bad DSS WRITE!!!!!!!!'
    fid.done()

def archiveRandomHindcasts(aepList, n, patternName, recordID, templateDbFileName, archiveDbFile):

    nStr = str(n).zfill(3)

    archiveDb = SqliteDatabase(archiveDbFile, SqliteDatabase.CREATION_MODE.CREATE_NEW_OR_OPEN_EXISTING_UPDATE)
    tempDb = SqliteDatabase(templateDbFileName, SqliteDatabase.CREATION_MODE.CREATE_NEW_OR_OPEN_EXISTING_UPDATE)

    for aep in aepList:

        # Define version identifier used to query the template database
        version = 'C:000%s|%s' %(aep, patternName)
        versionID = VersionIdentifier("american.FOLSOM-POOL", "flow",version)

        # Query the template database for this AEP
        eTs = tempDb.getEnsembleTimeSeries(versionID)
        issueDates = list(eTs.getIssueDates())

        # Define output ensemble time series
        randomID = 'C:000%s|RV-%s-%s' %(aep, patternName, nStr)
        newEts = EnsembleTimeSeries(recordID, "cfs","PER-AVER", randomID)

        for issueDate in issueDates:

            # Get nth ranked member from ensemble time series
            e = eTs.getEnsemble(issueDate)
            nthRandkedMember = e.getValues()

            # Define new ensemble for the randomly selected member
            selectedEnsemble = Ensemble(issueDate, nthRandkedMember, e.getStartDateTime(), Duration.ofHours(1), "cfs" )

            # Add selected Ensemble to random ensemble database
            newEts.addEnsemble(selectedEnsemble)

        archiveDb.write(newEts)

    tempDb.deleteAllEnsemblesFromDB()
    tempDb.close()
    archiveDb.close()

memberLookup = dict(zip(range(0,41), range(1980,2021)))
patternLookup = {'1986':'X3WM', '1997':'Y2WM'}
simNameLookup = {'1986':'X3WM_HC', '1997':'Y2WM_HC'}
altNameLookup = {'1986':'RV-X3WM', '1997':'RV-Y2WM'}

def main(baseRoot):

    # Define output directory for archived DSS files and sqlite databases
    resultsDir = r"%s\resultsEST" %(baseRoot)

    # Define paths to res sim model directories
    modelDir = os.path.join(baseRoot,'models' ,'R703F3_SOU_RV_20230929')
    watershedWkspFile = r"%s/R703F3_SOU_RV_20230929.wksp" %(modelDir)
    databaseDir = os.path.join(modelDir,'shared','EST_DBs')

    if not os.path.exists(resultsDir):
        os.makedirs(resultsDir)

    for pattern in ['1986', '1997']:

        # Define ResSim parameters
        altName = altNameLookup[pattern]
        simName = simNameLookup[pattern]
        patternName = patternLookup[pattern]

        simulationDssDir = os.path.join(modelDir,'rss',simName)
        archiveDbFile = r"%s\RV-%s_results.db" %(resultsDir, patternName)
        cleanExtractDssPath = r"%s\staticFiles\%s\simulation.dss" %(baseRoot, patternName)

        # Set Up logging file to keep track of the selected members
        loggingFile = r"%s\logs\RV-%s_randomShuffle.log" %(baseRoot, patternName)
        loggerMain = myLogger("Begin Processing %s (%s)..." %(pattern, patternName), loggingFile)
        loggerMain.info("Starting Ensemble Statistic Test...")

        # Define Record ID for creating new ensemble time series
        recordID = RecordIdentifier("american.FOLSOM-POOL","flow")

        # Define AEP list for testing
        aepList = list(range(200,550,50))

        # Configure ResSim for headless work
        simMode, simRun, rmiWksp, user = configureResSim(watershedWkspFile, simName, altName)

        # Outer loop that defines the nth ranked volume being tested
        for rankedMember in range(0,21):

            loggerN = myLogger("Ranked Volume: %s" %(rankedMember), loggingFile)
            loggerN.info("Processing....")

            # Move clean extract DSS file to rss folder
            shutil.copy(cleanExtractDssPath,simulationDssDir)

            # Define input database the limited ensemble time series (21-members for each issue date)
            inputDB = r"%s/%s_all_AEPs_topFiftyPercent.db" %(databaseDir, pattern)
            db = SqliteDatabase(inputDB, SqliteDatabase.CREATION_MODE.OPEN_EXISTING_UPDATE)

            # Define Target database that will contain the random members (1-member for each issue date)
            templateDbFileName = r"%s/template.db" %(databaseDir)
            tempDb = SqliteDatabase(templateDbFileName, SqliteDatabase.CREATION_MODE.CREATE_NEW_OR_OPEN_EXISTING_UPDATE)

            # Loop through each AEP
            for aep in aepList:

                # Log which is currently being worked on
                loggerAep = myLogger("scaling: %s" %(aep), loggingFile)
                loggerAep.info('Processing %s aep....'  %(aep))

                # Define version identifier used to query the limited ensemble time series database
                version = "%s_%s" %(pattern, aep)
                versionID = VersionIdentifier("american.FOLSOM-POOL", "flow",version)

                # Query the full ensemble database for this AEP
                eTs = db.getEnsembleTimeSeries(versionID)

                # Define list of all forecast issuance
                issueDates = list(eTs.getIssueDates())
                randomID = 'C:000%s|%s' %(aep, patternName)

                # Define ensemble time series for random ensemble members (1-member per issue date)
                newEts = EnsembleTimeSeries(recordID, "cfs","PER-AVER", randomID)

                for issueDate in issueDates:

                    # Get ensemble from the limited 21-member ensemble time series
                    e = eTs.getEnsemble(issueDate)
                    selectedMember = e.getValues()[rankedMember:rankedMember+1]

                    # Define new ensemble for the nth ranked member
                    selectedEnsemble = Ensemble(issueDate, selectedMember, e.getStartDateTime(), Duration.ofHours(1), "cfs" )

                    # Add nth ranked ensemble member to new ensemble time series
                    newEts.addEnsemble(selectedEnsemble)

                # Write nth ranked ensemble member time series to template db
                tempDb.write(newEts)

            tempDb.close()
            db.close()

            # Run ResSim
            simMode.computeRun(simRun, -1)

            # Release all file locks
            HecDSSFileDataManager().closeAllFiles()

            # Archive DSS results
            archiveSimulationResults(aepList, pattern, patternName, resultsDir, simulationDssDir,rankedMember)

            # Archive sqlite db results
            archiveRandomHindcasts(aepList, rankedMember, patternName, recordID, templateDbFileName, archiveDbFile)

            # Delete simulation and template database for next simulation
            HecDSSFileDataManager().closeAllFiles()

            # Delete simulation files for next iteration
            os.remove(r"%s\simulation.dss" %(simulationDssDir))
            os.remove(templateDbFileName)

        # Close workspace
        rmiWksp.closeWorkspace(user)

    sys.exit("Finished Computing!")


if __name__ == '__main__':

    baseRoot = r"C:\workspace\Folsom\FolsomSOU-EnsembleStatisticTest"

    main(baseRoot)