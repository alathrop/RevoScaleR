# Copyright (c) Microsoft Corporation.  All rights reserved.
#
# A preview of high-performance ‘big data’ analytics for SQL Server 2016 using 
# Microsoft R Server’s RevoScaleR package is included in Microsoft SQL Server 2016 
# Community Technology Preview 3 (CTP3).
#
# This script is provided as setup for running the SQL Server IOQ test suite, which
# can be used as part of your Installation/Operational Qualification protocol.
#
# In this script, specify setup information for your local client and SQL Server database
# At the end of this script, the tests in the SqlServerIOQ_Tests.R script (located in
# the inst/DemoScripts directory of the RevoScaleR installed package) are run.
# An HTML report is generated.
############################################################################################

require(RUnit)
require(RevoScaleR)

# Note: for improved security, read connection string from a file, such as
# connectionString <- readLines("sqlConnString.txt")

rxTestArgs <- list(
    # Compute context specifications
	connectionString =  "Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=MyUser;Pwd=MyPassword",
    consoleOutput = FALSE,
    wait = TRUE,
    autoCleanup = TRUE,
    ### Additional args for tests
    rxTestOdbc = FALSE,
	rxHaveWritePermission = TRUE  # Set to FALSE to run only tests that read data
  )  

options(rxTestArgs = rxTestArgs)
rm(rxTestArgs)

# Run the tests
RevoScaleR:::rxuRunTests( 
    testDir = rxGetOption("demoScriptsDir"),
    testFileRegexp = "SqlServerIOQ_Tests.R$")
#
# If a test fails due to a temporary problem (such as with the connection to the cluster), 
# rerun the single tests. As an example:
 
#RevoScaleR:::rxuRunTests( 
    #testDir = rxGetOption("demoScriptsDir"),
    #testFuncRegexp = "test.sqlserver.analytics.rxCube", 
    #testFileRegexp = "SqlServerIOQ_Tests.R")

