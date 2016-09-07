# Copyright (c) Microsoft Corporation.  All rights reserved.
#
# This script is provided as setup for running the Hadoop IOQ test suite, which
# can be used as part of your Installation/Operational Qualification protocol.
#
# In this script, specify setup information for your local client and hadoop cluster
# At the end of this script, the tests in the HadoopIOQ_Tests.R script (located in
# the inst/DemoScripts directory of the RevoScaleR installed package) are run.
# An HTML report is generated.
############################################################################################

require(RUnit)
require(RevoScaleR)

rxTestArgs <- list(
    # Specifications for compute context/file system
    nameNode = "sandbox.hortonworks.com", # Specify nameNode here
    hdfsShareDir = paste( "/user/RevoShare", Sys.info()[["user"]], sep="/" ),
    shareDir = paste( "/var/RevoShare", Sys.info()[["user"]], sep="/" ),
    #clientShareDir = rxGetDefaultTmpDirByOS(),
    sshUsername = Sys.info()[["user"]],
    sshHostname = "sandbox.hortonworks.com",
    sshSwitches = "",
    sshProfileScript = "/home/dev/.bash_profile",
    sshClientDir = "",

    jobTrackerURL = NULL,
    port = 8020,
    onClusterNode = TRUE,
    consoleOutput = FALSE,
    showOutputWhileWaiting = TRUE,
    autoCleanup = TRUE,
    workingDir = NULL,
    dataPath = NULL,
    outDataPath = NULL,
    fileSystem = NULL,
    packagesToLoad = NULL,
    resultsTimeout = 15,
    # Other variables used in tests

    myOmitLongTests = TRUE,
    myHdfsNativeDir = "/tmp",  # Used for copying files from local machine to Hadoop cluster
    myHdfsAirDemoCsvSubdir = "IOQ/AirlineDemoSmallCsv",
    myHdfsAirDemoXdfSubdir = "IOQ/AirlineDemoSmallXdf",
    myHdfsTestOuputSubdir = "IOQ/TempTestOutputXdf",

    createHadoopData = TRUE,  # Copy data to Hadoop cluster & import xdf file as first step
    removeHadoopDataOnCompletion = FALSE, # Remove data from Hadoop cluster on completion
    removeLocalDataOnCompletion = FALSE, # Remove data from local directory when completed
    localTestDataDir = "/tmp", # Local directory for writing data; must exist
    platform = "spark",
    numExecutors = 2,
    executorCores = 2,
    executorMem = "1g",
    driverMem = "1g",
    executorOverheadMem = "1g",
    extraSparkConfig = ""
    )
   
    
rxTestArgs$fileSystem <-
 RxHdfsFileSystem(hostName = rxTestArgs$nameNode, port = rxTestArgs$port)    
options(rxTestArgs = rxTestArgs)
rm(rxTestArgs)

# Run the tests
RevoScaleR:::rxuRunTests( 
    testDir = rxGetOption("demoScriptsDir"),
    testFileRegexp = "HadoopIOQ_Tests.R$")

# If a test fails due to a temporary problem (such as with the connection to the cluster), 
# rerun the single tests. As an example:
 
#RevoScaleR:::rxuRunTests( 
    #testDir = rxGetOption("demoScriptsDir"),
    #testFuncRegexp = "test.hadoop.rxKmeans.args.outFile", 
    #testFileRegexp = "HadoopIOQ_Tests.R")
