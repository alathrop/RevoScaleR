# Copyright (c) Microsoft Corporation.  All rights reserved.
#
# This script is provided as setup for running the Teradata IOQ test suite, which
# can be used as part of your Installation/Operational Qualification protocol.
#
# In this script, specify setup information for your local client and Teradata database
# At the end of this script, the tests in the TeradataIOQ_Tests.R script (located in
# the inst/DemoScripts directory of the RevoScaleR installed package) are run.
# An HTML report is generated.
############################################################################################

require(RUnit)
require(RevoScaleR)

# Note: for improved security, read connection string from a file, such as
# connectionString = readLines("tdConnString.txt")

rxTestArgs <- list(
    # Compute context specifications
	connectionString = "Driver=Teradata;DBCNAME=mydbc;Uid=MyUid;pwd=myPwd;",
    shareDir = paste("c:\\AllShare\\", Sys.info()[["user"]], sep=""), # Make sure directory exists on client
    remoteShareDir = "/tmp/revoJobs",
	revoPath = "/usr/lib64/MRO-for-MRS-8.0.3/R-3.2.2/lib64/R"
    consoleOutput = FALSE,
    wait = TRUE,
    autoCleanup = TRUE,
    # Other arguments used in tests
    rxNumAmps = 36 # Specify number of AMPS 
  )  

options(rxTestArgs = rxTestArgs)
rm(rxTestArgs)
# Run the tests
RevoScaleR:::rxuRunTests( 
    testDir = rxGetOption("demoScriptsDir"),
    testFileRegexp = "TeradataIOQ_Tests.R$")
#
# If a test fails due to a temporary problem (such as with the connection to the cluster), 
# rerun the single tests. As an example:
 
#RevoScaleR:::rxuRunTests( 
    #testDir = rxGetOption("demoScriptsDir"),
    #testFuncRegexp = "test.teradata.analytics.rxCube", 
    #testFileRegexp = "TeradataIOQ_Tests.R")

