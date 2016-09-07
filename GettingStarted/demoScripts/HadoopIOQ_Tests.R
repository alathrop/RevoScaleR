
# Copyright (c) Microsoft Corporation.  All rights reserved.
#
# Run tests using both RxHadoopMR and local compute contexts
#
############################################################################################

# HadoopIOQ_Setup.R creates the getOption("rxTestArgs") required by these functions

#############################################################################################
# Function to get compute contexts
#############################################################################################
"getHadoopComputeContext" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See HadoopIOQ_SetupAndRun.R.")
    }
    if (!is.null(rxTestArgs$platform) && rxTestArgs$platform == "spark") {
	RxSpark(
		nameNode     = rxTestArgs$nameNode,
		port         = rxTestArgs$port,
		hdfsShareDir = rxTestArgs$hdfsShareDir,
		shareDir     = rxTestArgs$shareDir,
		sshUsername  = rxTestArgs$sshUsername, 
		sshHostname  = rxTestArgs$sshHostname, 
		sshSwitches  = rxTestArgs$sshSwitches,
		wait = TRUE,
		consoleOutput = rxTestArgs$consoleOutput,
		fileSystem    = rxTestArgs$fileSystem,
		sshProfileScript = rxTestArgs$sshProfileScript,
		sshClientDir  = rxTestArgs$sshClientDir,
		jobTrackerURL = rxTestArgs$jobTrackerURL,
		onClusterNode = rxTestArgs$onClusterNode,
		showOutputWhileWaiting = rxTestArgs$showOutputWhileWaiting,
		autoCleanup = rxTestArgs$autoCleanup,
		workingDir  = rxTestArgs$workingDir,
		dataPath    = rxTestArgs$dataPath,
		outDataPath = rxTestArgs$outDataPath,
		packagesToLoad = rxTestArgs$packagesToLoad,
		numExecutors = rxTestArgs$numExecutors,
		executorCores = rxTestArgs$executorCores,
		executorMem = rxTestArgs$executorMem,
		driverMem = rxTestArgs$driverMem,
		executorOverheadMem = rxTestArgs$executorOverheadMem,
		resultsTimeout = rxTestArgs$resultsTimeout)
    } else {
	RxHadoopMR(
		nameNode     = rxTestArgs$nameNode,
		port         = rxTestArgs$port,
		hadoopSwitches = rxTestArgs$hadoopSwitches,
		hdfsShareDir = rxTestArgs$hdfsShareDir,
		shareDir     = rxTestArgs$shareDir,
		sshUsername  = rxTestArgs$sshUsername, 
		sshHostname  = rxTestArgs$sshHostname, 
		sshSwitches  = rxTestArgs$sshSwitches,
		wait = TRUE,
		consoleOutput = rxTestArgs$consoleOutput,
		fileSystem    = rxTestArgs$fileSystem,
		sshProfileScript = rxTestArgs$sshProfileScript,
		hadoopRPath   = rxTestArgs$hadoopRPath,
		revoPath      = rxTestArgs$revoPath,
		sshClientDir  = rxTestArgs$sshClientDir,
		usingRunAsUserMode = rxTestArgs$usingRunAsUserMode,
		jobTrackerURL = rxTestArgs$jobTrackerURL,
		onClusterNode = rxTestArgs$onClusterNode,
		showOutputWhileWaiting = rxTestArgs$showOutputWhileWaiting,
		autoCleanup = rxTestArgs$autoCleanup,
		workingDir  = rxTestArgs$workingDir,
		dataPath    = rxTestArgs$dataPath,
		outDataPath = rxTestArgs$outDataPath,
		packagesToLoad = rxTestArgs$packagesToLoad,
		resultsTimeout = rxTestArgs$resultsTimeout)
    }
    
}

"getLocalComputeContext" <- function()
{
    RxLocalSeq()
}

#############################################################################################
# Functions to get data sources
#############################################################################################
"getAirDemoTextHdfsDS" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See HadoopIOQ_SetupAndRun.R.")
    }    
    airColInfo <- list(ArrDelay = list(type = "integer"),
        DayOfWeek = list(type = "factor", 
        levels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
            "Friday", "Saturday", "Sunday")))
    # In .setup, a directory 'AirlineDemoSmallXdf' will be created with the file
    
    RxTextData(file = file.path(rxTestArgs$hdfsShareDir, 
        rxTestArgs$myHdfsAirDemoCsvSubdir), 
        missingValueString = "M", 
        colInfo = airColInfo, 
        fileSystem = rxTestArgs$fileSystem)         
}

"getAirDemoXdfHdfsDS" <- function()
{
     rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Test settings must be in 'rxTestArgs' option.  See HadoopIOQ_SetupAndRun.R.")
    }   
    # In the first test a directory 'IOQ\AirlineDemoSmallXdf' will be created with a composite xdf file
    RxXdfData( file = file.path(rxTestArgs$hdfsShareDir,
        rxTestArgs$myHdfsAirDemoXdfSubdir),
        fileSystem = rxTestArgs$fileSystem)
}

"getTestOutFileHdfsDS" <- function( testFileDirName )
{
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See HadoopIOQ_SetupAndRun.R.")
    }       
    RxXdfData( file = file.path(rxTestArgs$hdfsShareDir, 
        rxTestArgs$myHdfsTestOuputSubdir, testFileDirName),
        fileSystem = rxTestArgs$fileSystem )
}

"removeTestOutFileHdfsDS" <- function( testDS )
{
    rxSetComputeContext(getHadoopComputeContext())
    rxHadoopRemoveDir(testDS@file)
    rxSetComputeContext(getLocalComputeContext()) 

}

"getAirDemoTextLocalDS" <- function()
{
    airColInfo <- list(
        ArrDelay = list(type = "integer"),
        DayOfWeek = list(type = "factor", 
        levels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
            "Friday", "Saturday", "Sunday")))
    RxTextData(file = file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.csv"),
        colInfo = airColInfo,
        missingValueString = "M", 
        fileSystem = "native")    
}

"getAirDemoXdfCompositeLocalDS" <- function()
{
	rxTestArgs <- getOption("rxTestArgs")
    RxXdfData( file = file.path(rxTestArgs$localTestDataDir, 
        "AirlineDemoSmallComposite"),
        createCompositeSet = TRUE,
        fileSystem = "native")        
}

"getTestCsvLocalPath" <- function( testFileName )
{
    rxTestArgs <- getOption("rxTestArgs")
    file.path(rxTestArgs$localTestDataDir, 
        testFileName)
}

"getAirDemoXdfLocalDS" <- function()
{
    RxXdfData(file = file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.xdf"),
              fileSystem = "native")
}

"getTestOutFileLocalDS" <- function( testFileDirName )
{
    RxXdfData( file = file.path(getOption("rxTestArgs")$localTestDataDir, 
        testFileDirName),
        createCompositeSet = TRUE,
        fileSystem = "native")
}

"getIsOnNode" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (!is.null(rxTestArgs$onClusterNode) && (rxTestArgs$onClusterNode == TRUE))
    {
        return( TRUE )
    }
    return( FALSE )
}

"removeTestOutFileLocalDS" <- function( testDS )
{
    unlink(testDS@file, recursive = TRUE)
}

#############################################################################################
# Tests to put data on Hadoop cluster
#############################################################################################

"test.hadoop.aaa.initializeHadoop" <- function()
{
    rxSetComputeContext(getHadoopComputeContext())
    # Get the curent version from the Hadoop cluster
    "getRevoVersion" <- function() 
    { 
        Revo.version
    } 

    versionOutput <- rxExec(getRevoVersion)
    print(paste("MRS version on Hadoop cluster:", versionOutput$rxElem1$version.string))
    
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See HadoopIOQ_SetupAndRun.R.")
    }   

    output1 <- capture.output(
        rxHadoopListFiles(rxTestArgs$hdfsShareDir) 
        )
    hasUserDir <- grepl(rxTestArgs$hdfsShareDir, output1)
    print("Hadoop Compute Context being used:")
    print(getHadoopComputeContext())
    
    if (sum(hasUserDir) == 0)
    {
        print("WARNING: hdfsShareDir does not exist or problem with compute context.")
    }
    
    # Create local composite xdf file
    rxSetComputeContext(getLocalComputeContext())
    rxImport(getAirDemoTextLocalDS(), outFile = getAirDemoXdfCompositeLocalDS(),
        rowsPerRead = 200000, blocksPerCompositeFile = 1, overwrite=TRUE)
    
            
    if (rxTestArgs$createHadoopData )
    {  
        # Divide the AirlineDemoSmall.csv file into pieces
        rxSetComputeContext(getLocalComputeContext())
        airDemoSmallLocalDir <- getTestCsvLocalPath("AirDemoSmallCsv")
        unlink(airDemoSmallLocalDir, recursive = TRUE)
        dir.create(airDemoSmallLocalDir)
        airDemoSmallLocal1 <- file.path(airDemoSmallLocalDir, "AirlineDemoSmallPart1.csv")
        airDemoSmallLocal2 <- file.path(airDemoSmallLocalDir, "AirlineDemoSmallPart2.csv")
        airDemoSmallLocal3 <- file.path(airDemoSmallLocalDir, "AirlineDemoSmallPart3.csv")
        
        rxDataStep(inData = getAirDemoXdfLocalDS(), outFile = airDemoSmallLocal1,
            startRow = 1, numRows = 200000, overwrite = TRUE)
        
        rxDataStep(inData = getAirDemoXdfLocalDS(), outFile = airDemoSmallLocal2,
            startRow = 200001, numRows = 200000, overwrite = TRUE)
        
        rxDataStep(inData = getAirDemoXdfLocalDS(), outFile = airDemoSmallLocal3,
            startRow = 400001, numRows = 200000, overwrite = TRUE)
            
        rxSetComputeContext(getHadoopComputeContext())            
        # Make directory in HDFS for writing temporary test data
        testOutputDir <- file.path(rxTestArgs$hdfsShareDir,
            rxTestArgs$myHdfsTestOuputSubdir)
        rxHadoopMakeDir(testOutputDir)
               
        # Destination path for files in Hadoop
        airDemoSmallCsvHdfsPath <- file.path(rxTestArgs$hdfsShareDir, 
            rxTestArgs$myHdfsAirDemoCsvSubdir)  
               
        # Remove the directory if it's already there 
        #rxHadoopListFiles(airDemoSmallCsvHdfsPath) 
        print("Trying to remove directories in case they already exist.")     
        rxHadoopRemoveDir(airDemoSmallCsvHdfsPath)  
        # Create the directory               
        rxHadoopMakeDir(airDemoSmallCsvHdfsPath)
        
        if (getIsOnNode() == TRUE)
        {
            rxHadoopCopyFromLocal(source = airDemoSmallLocal1, 
                dest = airDemoSmallCsvHdfsPath) 
            rxHadoopCopyFromLocal(source = airDemoSmallLocal2, 
                dest = airDemoSmallCsvHdfsPath)     
            rxHadoopCopyFromLocal(source = airDemoSmallLocal3, 
                dest = airDemoSmallCsvHdfsPath)                       
        }
        else
        {
            # Copy the file from local client
            rxHadoopCopyFromClient(source = airDemoSmallLocal1, 
                nativeTarget = rxTestArgs$myHdfsNativeDir,
                hdfsDest = airDemoSmallCsvHdfsPath, 
                computeContext = getHadoopComputeContext(), 
                sshUsername = rxTestArgs$sshUsername, 
                sshHostname = rxTestArgs$sshHostname, 
                sshSwitches = rxTestArgs$sshSwitches)
            
            rxHadoopCopyFromClient(source = airDemoSmallLocal2, 
                nativeTarget = rxTestArgs$myHdfsNativeDir,
                hdfsDest = airDemoSmallCsvHdfsPath, 
                computeContext = getHadoopComputeContext(), 
                sshUsername = rxTestArgs$sshUsername, 
                sshHostname = rxTestArgs$sshHostname, 
                sshSwitches = rxTestArgs$sshSwitches)
            
            rxHadoopCopyFromClient(source = airDemoSmallLocal3, 
                nativeTarget = rxTestArgs$myHdfsNativeDir,
                hdfsDest = airDemoSmallCsvHdfsPath, 
                computeContext = getHadoopComputeContext(), 
                sshUsername = rxTestArgs$sshUsername, 
                sshHostname = rxTestArgs$sshHostname, 
                sshSwitches = rxTestArgs$sshSwitches)                        
        }
        
        #rxHadoopListFiles(airDemoSmallCsvHdfsPath) 
         
        # Import the csv files to a composite xdf file in HDFS
        airDemoSmallXdfHdfsPath <- file.path(rxTestArgs$hdfsShareDir, 
            rxTestArgs$myHdfsAirDemoXdfSubdir)
        rxHadoopRemoveDir(airDemoSmallXdfHdfsPath) # Remove directory if it's already there
        rxImport(inData = getAirDemoTextHdfsDS(), outFile = getAirDemoXdfHdfsDS(),  
            rowsPerRead = 200000, overwrite = TRUE)
        #rxHadoopListFiles(airDemoSmallXdfHdfsPath) 
        # Should have a composite file with three xdfd files
        #rxHadoopListFiles(file.path(airDemoSmallXdfHdfsPath, "data"))

    }  
    rxSetComputeContext(getLocalComputeContext())    
}

#############################################################################################
# Tests for getting file information
#############################################################################################

"test.hadoop.rxGetInfo" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # Test simple case
    rxSetComputeContext( getLocalComputeContext() )
    localInfo <- rxGetInfo( data = getAirDemoXdfLocalDS())
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopInfo <- rxGetInfo( data = getAirDemoXdfHdfsDS() )
    
    checkEquals(localInfo$numRows, hadoopInfo$numRows)
    checkEquals(localInfo$numVars, hadoopInfo$numVars)
    
    # Test with arguments
    rxSetComputeContext( getLocalComputeContext() )
    localInfoArgs <- rxGetInfo( data = getAirDemoXdfLocalDS(), 
        getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopInfoArgs <- rxGetInfo( data = getAirDemoXdfHdfsDS(),
         getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)    
    
    checkIdentical(localInfoArgs$varInfo, hadoopInfoArgs$varInfo)
    # rowsPerBlock only be equal if there is only one composite data file
    # data is generally retrieved in a different order
    checkEquals(nrow(localInfoArgs$data), nrow(hadoopInfoArgs$data))
    checkEquals(names(localInfoArgs$data), names(hadoopInfoArgs$data))
    rxSetComputeContext( getLocalComputeContext() )
}

# Note:	rxGetInfo is not defined for text file for some parameters
#		For RxSpark context text file is actually converted into xdf before calling rxGetInfo 			
"test.hadoop.rxGetInfo.data.xdf" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
       
    # Test using xdf Data Sources
    rxSetComputeContext( getLocalComputeContext() )
    localInfoArgs <- rxGetInfo( data = getAirDemoXdfCompositeLocalDS(), 
        getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopInfoArgs <- rxGetInfo( data = getAirDemoXdfHdfsDS(),
         getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)    
    
    checkIdentical(localInfoArgs$varInfo, hadoopInfoArgs$varInfo)

    # Data order may vary
    checkEquals(nrow(localInfoArgs$data), nrow(hadoopInfoArgs$data))
    checkEquals(names(localInfoArgs$data), names(hadoopInfoArgs$data))
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxGetVarInfo" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localVarInfo <- rxGetVarInfo( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopVarInfo <- rxGetVarInfo( getAirDemoXdfHdfsDS() )
    
    checkIdentical(localVarInfo, hadoopVarInfo)
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxGetVarNames" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localVarNames <- rxGetVarNames( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopVarNames <- rxGetVarNames( getAirDemoXdfHdfsDS() )
    
    checkEquals(localVarNames, hadoopVarNames)
    
    rxSetComputeContext( getLocalComputeContext() ) 
}

"test.hadoop.rxLocateFile" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # Local tests
    rxSetComputeContext( getLocalComputeContext() )
    
    # CSV file

    dataSource <- getAirDemoTextLocalDS()
    localFileName <- basename(dataSource@file)
    localDir <- dirname(dataSource@file)
    localLocated <- rxLocateFile(file = localFileName, pathsToSearch = localDir)
    checkEquals(normalizePath(localLocated), normalizePath(dataSource@file))
    
    dataSource <- getAirDemoXdfLocalDS()
    localFileName <- basename(dataSource@file)
    localDir <- dirname(dataSource@file)
    localLocated <- rxLocateFile(file = localFileName, pathsToSearch = localDir)
    checkEquals(normalizePath(localLocated), normalizePath(dataSource@file))
    
    # Hadoop
    # Csv
    rxSetComputeContext( getHadoopComputeContext() )
    dataSource <- getAirDemoTextHdfsDS()
    hadoopFileName <- basename(dataSource@file)
    hadoopDir <- dirname(dataSource@file)
    hadoopLocated <- rxLocateFile(file = hadoopFileName, pathsToSearch = hadoopDir, 
        defaultExt = "", fileSystem = dataSource@fileSystem)
    checkEquals(hadoopLocated, dataSource@file)
    
    # Xdf
    dataSource <- getAirDemoXdfHdfsDS()
    hadoopFileName <- basename(dataSource@file)
    hadoopDir <- dirname(dataSource@file)
    hadoopLocated <- rxLocateFile(file = hadoopFileName, pathsToSearch = hadoopDir, 
        defaultExt = ".xdf", fileSystem = dataSource@fileSystem)
    checkEquals(hadoopLocated, dataSource@file)
    
    rxSetComputeContext( getLocalComputeContext() )
}
 

"test.hadoop.rxReadXdf" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localDataFrame <- rxReadXdf( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopDataFrame <- rxReadXdf( getAirDemoXdfHdfsDS() )
    
    rowOrder <- order(localDataFrame$ArrDelay, localDataFrame$CRSDepTime, 
        localDataFrame$DayOfWeek)
    localDataFrame <- localDataFrame[rowOrder,]
    row.names(localDataFrame) <- NULL
      
    rowOrder <- order(hadoopDataFrame$ArrDelay, hadoopDataFrame$CRSDepTime, 
        hadoopDataFrame$DayOfWeek)
    hadoopDataFrame <- hadoopDataFrame[rowOrder,]   
    row.names(hadoopDataFrame) <- NULL     
    
    checkIdentical(localDataFrame, hadoopDataFrame)
    
    rxSetComputeContext( getLocalComputeContext() )
}

#############################################################################################
# Tests for plotting functions
#############################################################################################
"test.hadoop.rxHistogram" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~ArrDelay, data = getAirDemoXdfLocalDS(), 
        rowSelection = ArrDelay > 0, title = "Local Histogram" )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopHistogram <- rxHistogram(~ArrDelay, data = getAirDemoXdfHdfsDS(), 
        rowSelection = ArrDelay > 0, title = "Hadoop Histogram" )
    
    checkIdentical(localHistogram$panel.args, hadoopHistogram$panel.args)
    checkIdentical(localHistogram$call, hadoopHistogram$call)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxLinePlot" <- function()
{  
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localLinePlot <- rxLinePlot(ArrDelay~CRSDepTime, data = getAirDemoXdfLocalDS(), 
         rowSelection = ArrDelay > 60, type = "p", title = "Local Line Plot" )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopLinePlot <- rxLinePlot(ArrDelay~CRSDepTime,, data = getAirDemoXdfHdfsDS(), 
        rowSelection = ArrDelay > 60, type = "p", title = "Hadoop Line Plot" )
    localLinePlot$panel.args[[1]]$x <- sort(localLinePlot$panel.args[[1]]$x)
    localLinePlot$panel.args[[1]]$y <- sort(localLinePlot$panel.args[[1]]$y)
    hadoopLinePlot$panel.args[[1]]$x <- sort(hadoopLinePlot$panel.args[[1]]$x)
    hadoopLinePlot$panel.args[[1]]$y <- sort(hadoopLinePlot$panel.args[[1]]$y)
    checkIdentical(localLinePlot$panel.args, hadoopLinePlot$panel.args)
    
    hadoopCsvLinePlot <- rxLinePlot(ArrDelay~CRSDepTime,, data = getAirDemoTextHdfsDS(), 
        rowSelection = ArrDelay > 60, type = "p", title = "Hadoop Line Plot from Text File" )
    hadoopCsvLinePlot$panel.args[[1]]$x <- sort(hadoopCsvLinePlot$panel.args[[1]]$x)
    hadoopCsvLinePlot$panel.args[[1]]$y <- sort(hadoopCsvLinePlot$panel.args[[1]]$y)
    checkIdentical(localLinePlot$panel.args, hadoopCsvLinePlot$panel.args)
   
    rxSetComputeContext( getLocalComputeContext() )
}

#############################################################################################
# Tests for data step
#############################################################################################
"test.hadoop.rxDataStep.data.XdfToXdf" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # Run example locally
    rxSetComputeContext( getLocalComputeContext() )
    outLocalDS <- getTestOutFileLocalDS("DataStepTest")
    rxDataStep(inData = getAirDemoXdfLocalDS(), 
        outFile = outLocalDS, overwrite = TRUE)
    on.exit(removeTestOutFileLocalDS( outLocalDS ), add = TRUE)
    localInfo <- rxGetInfo(data = outLocalDS, getVarInfo = TRUE)

    rxSetComputeContext( getHadoopComputeContext() )
    outHdfsDS <- getTestOutFileHdfsDS("DataStepTest")
    rxDataStep(inData = getAirDemoXdfHdfsDS(),
        outFile = outHdfsDS, overwrite = TRUE)
    hadoopInfo = rxGetInfo(data = outHdfsDS, getVarInfo = TRUE)
    
    checkEquals(localInfo$numRows, hadoopInfo$numRows)
    checkIdentical(localInfo$varInfo, hadoopInfo$varInfo)
   
    # Clean-up
    removeTestOutFileHdfsDS( outHdfsDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )
}

"test.hadoop.rxDataStep.data.XdfToDataFrame" <- function()
{     
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # Note: row order in data frames will differ
    rxSetComputeContext( getLocalComputeContext() )
    localDataFrame <- rxDataStep(inData = getAirDemoXdfLocalDS())
    rowOrder <- order(localDataFrame$ArrDelay, localDataFrame$CRSDepTime, 
        localDataFrame$DayOfWeek)
    localDataFrame <- localDataFrame[rowOrder,]
    row.names(localDataFrame) <- NULL

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopDataFrame <- rxDataStep(inData = getAirDemoXdfHdfsDS())
    rowOrder <- order(hadoopDataFrame$ArrDelay, hadoopDataFrame$CRSDepTime, 
        hadoopDataFrame$DayOfWeek)
    hadoopDataFrame <- hadoopDataFrame[rowOrder,]
    row.names(hadoopDataFrame) <- NULL
    checkIdentical(localDataFrame, hadoopDataFrame)
   
    rxSetComputeContext( getLocalComputeContext() )
}



#############################################################################################
# Tests for analysis functions that read data
#############################################################################################
"test.hadoop.rxCube.data.xdf" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxCube with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localCube <- rxCube(ArrDelay~DayOfWeek, data = getAirDemoXdfLocalDS())

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCube <- rxCube(ArrDelay~DayOfWeek, data = getAirDemoXdfHdfsDS() )
    
    checkIdentical(localCube$Counts, hadoopCube$Counts)
   
    rxSetComputeContext( getLocalComputeContext() )
}
    
"test.hadoop.rxCube.data.data.frame" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )    
    ### rxCube with input data frame
 
    rxSetComputeContext( getLocalComputeContext() )
    localCube <- rxCube(~Species, data = iris)

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCube <- rxCube(~Species, data = iris)
    
    checkIdentical(localCube$Counts, hadoopCube$Counts)    
    rxSetComputeContext( getLocalComputeContext())
}

"test.hadoop.rxCrossTabs" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxCrosstabs with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localCrossTabs <- rxCrossTabs(ArrDelay10~DayOfWeek, data = getAirDemoXdfLocalDS(),
		rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = ArrDelay*10) )
	
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCrossTabs <- rxCrossTabs(ArrDelay10~DayOfWeek, data = getAirDemoXdfHdfsDS(),
		rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = ArrDelay*10) )
    
    checkIdentical(localCrossTabs$sums, hadoopCrossTabs$sums)
	checkIdentical(localCrossTabs$counts, hadoopCrossTabs$counts)
	checkIdentical(localCrossTabs$chisquare, hadoopCrossTabs$chisquare)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxSummary" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxSummary with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localSummary <- rxSummary(ArrDelay10~DayOfWeek, data = getAirDemoXdfLocalDS(), 
		rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopSummary <- rxSummary(ArrDelay10~DayOfWeek, data = getAirDemoXdfHdfsDS(),
        rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))
    
    checkEquals(localSummary$categorical[[1]], hadoopSummary$categorical[[1]])
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxQuantile" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxQuantile with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localQuantile <- rxQuantile("ArrDelay", data = getAirDemoXdfLocalDS()) 

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopQuantile <- rxQuantile("ArrDelay", data = getAirDemoXdfHdfsDS())
    
    checkEquals(localQuantile, hadoopQuantile)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxLinMod" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxLinMod with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoXdfLocalDS(), 
		rowSelection = CRSDepTime > 10)

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoXdfHdfsDS(),
        rowSelection = CRSDepTime > 10)
    
    checkEquals(localLinMod$coefficients, hadoopLinMod$coefficients)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxLinMod.data.csv" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxLinMod with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoTextLocalDS(), 
		rowSelection = CRSDepTime > 10)

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoTextHdfsDS(),
        rowSelection = CRSDepTime > 10)
    
    checkEquals(localLinMod$coefficients, hadoopLinMod$coefficients)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxCovCor" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxCovCor with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localCovCor <- rxCovCor(~ArrDelay+DayOfWeek+CRSDepTime10, data = getAirDemoXdfLocalDS(), 
		transforms = list(CRSDepTime10 = 10*CRSDepTime))

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCovCor <- rxCovCor(~ArrDelay+DayOfWeek+CRSDepTime10, data = getAirDemoXdfHdfsDS(), 
		transforms = list(CRSDepTime10 = 10*CRSDepTime))
    
    checkEquals(localCovCor$CovCor, hadoopCovCor$CovCor)
	checkEquals(localCovCor$StdDevs, hadoopCovCor$StdDevs)
	checkEquals(localCovCor$Means, hadoopCovCor$Means)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxGlm" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxGlm with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
		
    localRxGlm <- rxGlm(ArrOnTime ~ CRSDepTime, data = getAirDemoXdfLocalDS(), 
		family = binomial(link = "probit"), transforms = list(ArrOnTime = ArrDelay < 2))

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopRxGlm <- rxGlm(ArrOnTime ~ CRSDepTime, data = getAirDemoXdfHdfsDS(), 
		family = binomial(link = "probit"), transforms = list(ArrOnTime = ArrDelay < 2))
    
    checkEquals(localRxGlm$coefficients, hadoopRxGlm$coefficients)
	checkEquals(localRxGlm$coef.std.error, hadoopRxGlm$coef.std.error)
	checkEquals(localRxGlm$df, hadoopRxGlm$df)
   
    rxSetComputeContext( getLocalComputeContext() )
}


#############################################################################################
# Tests for analysis functions that write data
#############################################################################################

"test.hadoop.rxLogit.rxPredict" <- function()
{
    
	on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
	### Local computations
    rxSetComputeContext( getLocalComputeContext() )
	inLocalDS <- getAirDemoXdfLocalDS()
	outLocalDS <- getTestOutFileLocalDS("LogitPred")
	
	# Logistic regression
    localLogit <- rxLogit(ArrDel15~DayOfWeek, data = inLocalDS,
		transforms = list(ArrDel15 = ArrDelay > 15), maxIterations = 6 )
		
	# Predictions
    localPredOut <- rxPredict(modelObject = localLogit, data = inLocalDS, 
        writeModelVars = TRUE, predVarNames = "delayPred1", outData = outLocalDS, overwrite = TRUE) 
	
	outLocalVarInfo <- rxGetVarInfo( outLocalDS )
	
    localPredOut2 <- rxPredict(modelObject = localLogit, data = inLocalDS, 
        writeModelVars = TRUE, predVarNames = "delayPred1", extraVarsToWrite = "CRSDepTime",
        outData = outLocalDS, overwrite = TRUE) 
	
	outLocalVarInfo2 <- rxGetVarInfo( outLocalDS )   
    
    localPredOut3 <- rxPredict(modelObject = localLogit, data = inLocalDS, 
        writeModelVars = FALSE, predVarNames = "delayPred1", extraVarsToWrite = c("ArrDelay","CRSDepTime"),
        outData = outLocalDS, overwrite = TRUE) 
	
	outLocalVarInfo3 <- rxGetVarInfo( outLocalDS )          
	
	### Hadoop computations
    rxSetComputeContext( getHadoopComputeContext() )
	inHadoopDS <- getAirDemoXdfHdfsDS()
	outHadoopDS <- getTestOutFileHdfsDS("LogitPred")
	
	# Logistic regression
    hadoopLogit <- rxLogit(ArrDel15~DayOfWeek, data = inHadoopDS,
		transforms = list(ArrDel15 = ArrDelay > 15), maxIterations = 6 )
	
	checkEquals(hadoopLogit$coefficients, localLogit$coefficients)
	checkEquals(hadoopLogit$coef.std.error, localLogit$coef.std.error)
		
	# Predictions
    hadoopPredOut <- rxPredict(modelObject = hadoopLogit, data = inHadoopDS, 
        writeModelVars = TRUE, predVarNames = "delayPred1", outData = outHadoopDS, overwrite = TRUE) 
	
	outHadoopVarInfo <- rxGetVarInfo( outHadoopDS )	
    
    hadoopPredOut2 <- rxPredict(modelObject = hadoopLogit, data = inHadoopDS, 
        writeModelVars = TRUE, predVarNames = "delayPred1", extraVarsToWrite = "CRSDepTime",
        outData = outHadoopDS, overwrite = TRUE) 
	
	outHadoopVarInfo2 <- rxGetVarInfo( outHadoopDS )   
    
    hadoopPredOut3 <- rxPredict(modelObject = hadoopLogit, data = inHadoopDS, 
        writeModelVars = FALSE, predVarNames = "delayPred1", extraVarsToWrite = c("ArrDelay","CRSDepTime"),
        outData = outHadoopDS, overwrite = TRUE) 
	
	outHadoopVarInfo3 <- rxGetVarInfo( outHadoopDS )     
    
    checkEquals(outHadoopVarInfo$delayPred1, outLocalVarInfo$delayPred1)
    checkEquals(outHadoopVarInfo$ArrDelay, outLocalVarInfo$ArrDelay)
    
    checkEquals(length(outHadoopVarInfo2), length(outLocalVarInfo2))
    checkEquals(outHadoopVarInfo2$ArrDelay, outLocalVarInfo2$ArrDelay)
    
    checkEquals(length(outHadoopVarInfo3), length(outLocalVarInfo3))
    checkEquals(outHadoopVarInfo3$ArrDelay, outLocalVarInfo3$ArrDelay)
    checkEquals(outHadoopVarInfo3$CRSDepTime, outLocalVarInfo3$CRSDepTime)
    
    # Clean-up
    removeTestOutFileHdfsDS( outHadoopDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )    
	
}

"test.hadoop.rxKmeans.args.outFile" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
	testCenters <- matrix(c(13, 20, 7, 17, 10), ncol = 1)
	
	### Local computations
    rxSetComputeContext( getLocalComputeContext() )
    outLocalDS <- getTestOutFileLocalDS("KmeansOut")
	
    localKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoXdfLocalDS(), outFile = outLocalDS,
        centers = testCenters, writeModelVars = TRUE, overwrite = TRUE)
	
	localInfo <- rxGetInfo(outLocalDS, getVarInfo = TRUE)
	
    ### Hadoop computations
	rxSetComputeContext( getHadoopComputeContext() )
    outHadoopDS <- getTestOutFileHdfsDS("KmeansOut")
	
    hadoopKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoXdfHdfsDS(), outFile = outHadoopDS,
        centers = testCenters, writeModelVars = TRUE, overwrite = TRUE)
	
	hadoopInfo <- rxGetInfo(outHadoopDS, getVarInfo = TRUE)
    
    # Comparison tests
	checkEquals(hadoopInfo$numRows, localInfo$numRows)
	checkIdentical(hadoopInfo$varInfo, localInfo$varInfo)
	checkEquals(hadoopKmeans$centers, localKmeans$centers)
    
    # Clean-up
    removeTestOutFileHdfsDS( outHadoopDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )      
   
}

"test.hadoop.rxDTree.regression" <- function()             
{
    # Purpose:   Test basic regression tree functionality on Hadoop 

    formula <- as.formula( "ArrDelay ~ CRSDepTime + DayOfWeek" )
    
    ### Local computations   
    rxSetComputeContext( getLocalComputeContext() ) 
    inLocalDS <- getAirDemoXdfLocalDS()
    outLocalDS <- getTestOutFileLocalDS("DTreeOut")         
    localDTree <- rxDTree( formula, data  = inLocalDS,
                        blocksPerRead  = 30,
                        minBucket      = 500,
                        maxDepth       = 10,
                        cp             = 0,
                        xVal           = 0,
                        maxCompete     = 0,
                        maxSurrogate   = 0,
                        maxNumBins     = 101,
                        verbose        = 1,
                        reportProgress = 0 ) 
                         
    # Compute predictions
    localPred <- rxPredict( localDTree, data = inLocalDS, outData = outLocalDS, overwrite = TRUE,
                                      verbose = 1, reportProgress = 0 )                           
    localPredSum <- rxSummary( ~ ArrDelay_Pred, data = outLocalDS )
    
    
    ### Hadoop computations   
    rxSetComputeContext( getHadoopComputeContext() )  
    inHadoopDS <- getAirDemoXdfHdfsDS()
    outHadoopDS <- getTestOutFileHdfsDS("DTreeOut")           
    hadoopDTree <- rxDTree( formula, data  = inHadoopDS,
                        blocksPerRead  = 30,
                        minBucket      = 500,
                        maxDepth       = 10,
                        cp             = 0,
                        xVal           = 0,
                        maxCompete     = 0,
                        maxSurrogate   = 0,
                        maxNumBins     = 101,
                        verbose        = 1,
                        reportProgress = 0 ) 
                         
    # Compute predictions

    hadoopPred <- rxPredict( hadoopDTree, data = inHadoopDS, outData = outHadoopDS, overwrite = TRUE,
                                      verbose = 1, reportProgress = 0 )                           
    hadoopPredSum <- rxSummary( ~ ArrDelay_Pred, data = outHadoopDS)    
    
    # Perform checks
    
    #checkEquals( sum( ADSrxdt$splits[ , "improve" ] ), 0.181093099185139 )  
    checkEquals(sum(localDTree$splits[ , "improve" ] ), sum(hadoopDTree$splits[ , "improve" ] ))
    
    localPrune <- prune(localDTree,  cp = 1e-6 )
    hadoopPrune <- prune(hadoopDTree,  cp = 1e-6 ) 
    checkEquals(sum(localPrune$splits[ , "improve" ] ), sum(hadoopPrune$splits[ , "improve" ] ))  
            
    checkEquals(localPredSum$sDataFrame, hadoopPredSum$sDataFrame)
    
    # Clean-up
    removeTestOutFileHdfsDS( outHadoopDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )             
                      
}

#############################################################################################
# rxExec Example
#############################################################################################
 "test.hadoop.rxExec" <- function()
{
	playCraps <- function()
    {
	    result <- NULL
	    point <- NULL
	    count <- 1
	    while (is.null(result))
	    {
		    roll <- sum(sample(6, 2, replace=TRUE))
    		
		    if (is.null(point))
		    {
			    point <- roll
		    }
		    if (count == 1 && (roll == 7 || roll == 11))
		    { 
			    result <- "Win"
		    }
 		    else if (count == 1 && (roll == 2 || roll == 3 || roll == 12)) 
		    {
			    result <- "Loss"
		    } 
		    else if (count > 1 && roll == 7 )
		    {
			    result <- "Loss"
		    } 
		    else if (count > 1 && point == roll)
		    {
			    result <- "Win"
		    } 
		    else
		    {
			    count <- count + 1
		    }
	    }
	    result
    }
	### Local computations   
    rxSetComputeContext( getLocalComputeContext() ) 
    localExec <- rxExec(playCraps, timesToRun=100, taskChunkSize=25)
	
	rxSetComputeContext( getHadoopComputeContext() ) 
	hadoopExec <- rxExec(playCraps, timesToRun=100, taskChunkSize=25)
	checkEquals(length(localExec), length(hadoopExec))
	checkEquals(length(localExec[[1]]), length(hadoopExec[[1]]))

}	

"test.hadoop.dataPath" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    localComputeContext <- getLocalComputeContext()
    localAirDS <- getAirDemoXdfLocalDS()
    localAirBase <- basename(localAirDS@file)
    localAirDir <- dirname(localAirDS@file)
    localComputeContext@dataPath <- localAirDir
    localAirDS@file <- localAirBase
    
    # rxSummary with input xdf file
    rxSetComputeContext( localComputeContext )
    localSummary <- rxSummary(ArrDelay10~DayOfWeek, data = localAirBase, 
		rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))

    hadoopComputeContext <- getHadoopComputeContext()
    hadoopAirDS <- getAirDemoXdfHdfsDS()
    hadoopAirBase <- basename(hadoopAirDS@file)
    hadoopAirDir <- dirname(hadoopAirDS@file)
    hadoopComputeContext@dataPath <- hadoopAirDir  
    hadoopAirDS@file <- hadoopAirBase  
    rxSetComputeContext( hadoopComputeContext )
    hadoopSummary <- rxSummary(ArrDelay10~DayOfWeek, data = hadoopAirDS,
        rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))
    
    checkEquals(localSummary$categorical[[1]], hadoopSummary$categorical[[1]])
   
    rxSetComputeContext( getLocalComputeContext() )
}

#############################################################################################
# Always run last: Test to remove data from Hadoop cluster
#############################################################################################
"test.hadoop.zzz.removeHadoopData" <- function()
{
    # Always remove temp data
    rxSetComputeContext(getHadoopComputeContext())
    testOutputDir <- file.path(getOption("rxTestArgs")$hdfsShareDir,
            getOption("rxTestArgs")$myHdfsTestOuputSubdir)
    rxHadoopRemoveDir(testOutputDir)
    if (getOption("rxTestArgs")$removeHadoopDataOnCompletion)
    {
        # Remove air data from Hadoop cluster
        
        airDemoSmallCsvHdfsPath <- file.path(getOption("rxTestArgs")$hdfsShareDir, 
            getOption("rxTestArgs")$myHdfsAirDemoCsvSubdir) 
        rxHadoopRemoveDir(airDemoSmallCsvHdfsPath) 
            
        airDemoSmallXdfHdfsPath <- file.path(getOption("rxTestArgs")$hdfsShareDir, 
            getOption("rxTestArgs")$myHdfsAirDemoXdfSubdir)
        rxHadoopRemoveDir(airDemoSmallXdfHdfsPath)          
    } 
    else
    {
        rxHadoopMakeDir(testOutputDir)
    }
    rxSetComputeContext(getLocalComputeContext()) 
}

