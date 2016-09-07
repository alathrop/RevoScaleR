
# Copyright (c) Microsoft Corporation.  All rights reserved.
#
# Run tests using both RxInSqlServer and local compute contexts and compare results
# See SqlServerIOQ_SetupAndRun.R for setup information
#
############################################################################################


#############################################################################################
# Function to get compute contexts
#############################################################################################
            
"getSqlServerComputeContext" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See SqlServerIOQ_SetupAndRun.R.")
    }
    
    testCluster <- RxInSqlServer( 
        connectionString = rxTestArgs$connectionString,
        numTasks = rxTestArgs$numTasks,
        #shareDir = rxTestArgs$shareDir,
        #remoteShareDir = rxTestArgs$remoteShareDir,
        revoPath = rxTestArgs$revoPath,
        consoleOutput = rxTestArgs$consoleOutput,
        wait = rxTestArgs$wait,
        autoCleanup = rxTestArgs$autoCleanup,
        traceEnabled = rxTestArgs$traceEnabled,
        traceLevel = rxTestArgs$traceLevel
    ) 

    return( testCluster )
}

"getLocalComputeContext" <- function()
{
    RxLocalSeq()
}

#############################################################################################
# Functions to get data sources
#############################################################################################
"getAirDemoSqlServerDS" <- function( specifyLevels = TRUE)
{
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See SqlServerIOQ_SetupAndRun.R.")
    }
    
    sqlConnString = rxTestArgs$connectionString
    airColInfo <- NULL
    if (specifyLevels == TRUE)
    {
        airColInfo <- list(
            ArrDelay = list(type = "integer", low = -86, high = 1490),
			CRSDepTime = list(low = .016667, high = 23.98333),
            DayOfWeek = list(type = "factor", 
                levels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
                "Friday", "Saturday", "Sunday")))
    }
    else
    {
         airColInfo <- list(
            ArrDelay = list(type = "integer"),
            DayOfWeek = list(type = "factor"),
            CRSDepTime = list(type = "float32"))
    }
       

    testDataSource <- RxSqlServerData(
        connectionString = sqlConnString, 
        table = "AirlineDemoSmall", 
        colInfo = airColInfo,
        rowsPerRead = 5000) 
        
    return(testDataSource)   
     
}

"getAirDemoXdfLocalDS" <- function()
{
    RxXdfData(file = file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.xdf"),
              fileSystem = "native")
}

"getTestOdbc" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (!is.null(rxTestArgs) && !is.null(rxTestArgs$rxTestOdbc) && (rxTestArgs$rxTestOdbc == TRUE))
    {
        return( TRUE )
    }
    return( FALSE)
}

"getHaveWritePermission" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (!is.null(rxTestArgs) && !is.null(rxTestArgs$rxHaveWritePermission) && 
		(rxTestArgs$rxHaveWritePermission == FALSE))
    {
        return(FALSE)
    }
    return( TRUE)
}
"rxuTestAnalysisFunctionWithAirDemo" <- function( analysisName, analysisParams, testValues,
    printFirstOutput = FALSE)
{        
    origComputeContext <- rxGetComputeContext()
    on.exit(rxSetComputeContext(origComputeContext), add = TRUE)
    
    localComputeContext <- getLocalComputeContext() 
    distributedComputeContext <- getSqlServerComputeContext()    
    localTestDataSource <- getAirDemoXdfLocalDS()
    sqlServerTestDataSource <- getAirDemoSqlServerDS()
   
    rxSetComputeContext(localComputeContext)

	analysisParams$data <- localTestDataSource
	localResult <- do.call(analysisName, analysisParams)
    if (printFirstOutput)
    {
        cat("\nLocal results are:\n")
        print(localResult)
	}
    else
    {
        out1 <- capture.output(print(localResult))
        cat("\n", out1[1], "\n")
        cat("\n", out1[2], "\n")
    }
    
    testOdbc <- getTestOdbc()
    if (testOdbc)
    {
        testDataSources <- list(sqlServerTestDataSource, sqlServerTestDataSource)
    }
    else
    {
        testDataSources <- list(sqlServerTestDataSource)
    }
    
      
	for (i in 1:length(testDataSources))
	{
		thisDataSource <- testDataSources[[i]]
        cat("-----------------------------------------------------------------------------\n")
        if (!testOdbc || (i == 2))
        {
            cat(paste("Testing RxInSqlServer compute context\n"))
            rxSetComputeContext( distributedComputeContext) 
            SqlServerType <- "InSqlServer"
        }
        else
        {
            cat(paste("Testing RxSqlServerData data source in local compute context\n"))
            SqlServerType <- "ODBC"
        }
        
		cat("\n")
		analysisParams$data <- testDataSources[[i]]
        tm <- system.time(
		    currentResult <- do.call(analysisName, analysisParams)
            )
        cat("Elapsed time for analysis:", tm["elapsed"], "\n")
        thisTime <- as.numeric(tm["elapsed"])
        if (!exists("rxSqlServerTimings"))
        {
            rxSqlServerTimings <<- data.frame(Analysis = analysisName,
                Type = SqlServerType, Time = thisTime, stringsAsFactors = FALSE)
        }
        else
        {
            newRow <- nrow(rxSqlServerTimings) + 1
            rxSqlServerTimings[newRow, "Analysis"] <<- analysisName
            rxSqlServerTimings[newRow, "Type"] <<- SqlServerType
            rxSqlServerTimings[newRow, "Time"] <<- thisTime
        }
        
              
        if (is.null(testValues))
        {
            cat(paste("    Testing for equal output.\n")) 
            checkEquals(localResult, currentResult)
        }
        else
        {
		    for (j in 1:length(testValues))
		    {
                currentTestValue <- currentResult[testValues[j]]
                cat(paste("    Testing output component:", testValues[j], "\n"))     
	            checkEquals(localResult[testValues[j]], currentTestValue)
		    }
        }
	}
	rxSetComputeContext( localComputeContext)	    
}

"rxuEnsureSameClientServerCollate" <- function()
{
    # ensure that all the server nodes have the same collate.
    clusterCollate <- function(checkUnique = TRUE)
    {
	    cCollate <- rxExec("Sys.getlocale", category = "LC_COLLATE")
        cCollate <- unlist(cCollate)
        
        if (checkUnique) {
            cCollate <- unique(cCollate)
            if (length(cCollate) != 1) {
                stop("LC_COLLATE is not the same for the nodes in current cluster context")
            }
        }
        
        return(cCollate)
    }
    
    origComputeContext <- rxGetComputeContext()
    on.exit(rxSetComputeContext(origComputeContext), add = TRUE)
    distributedComputeContext <- getSqlServerComputeContext()    
    rxSetComputeContext( distributedComputeContext) 
    
    # make sure LC_COLLATE is the same for both client and server,
    # which could affect sort() and how factor levels are ordered. 
    cCollate <- clusterCollate()
    lCollate <- Sys.getlocale("LC_COLLATE")
    if (lCollate != cCollate) {
        warning("changing the client collate (", lCollate, ") to the server collate (", cCollate, ").")
        Sys.setlocale("LC_COLLATE", cCollate)
    }
}
#rxuEnsureSameClientServerCollate()

#############################################################################################
# Core High-Performance Analytics Algorithms Available In-Database
#############################################################################################

"test.SqlServer.HPA.rxSummary" <- function()
{
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxSummary", 
	    analysisParams = list(
            formula = ArrDelay10~DayOfWeek, 
            transforms = substitute(list(ArrDelay10 = ArrDelay)),
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = c("categorical")
        )    
    
	# Underlying code:
	# Test code for local computations
    #rxSetComputeContext( getLocalComputeContext() )
    #localSummary <- rxSummary(ArrDelay10~DayOfWeek, data = getAirDemoXdfLocalDS(), 
	#	rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))
    #
	# Test code for in-database computations
    rxSetComputeContext( getSqlServerComputeContext() )
	myDataSource <- getAirDemoSqlServerDS()
    sqlServerSummary <- rxSummary(ArrDelay10~DayOfWeek, data = myDataSource,
       rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))	

               
}

"test.AAA.SqlServer" <- function()
{
	sqlCompute <- getSqlServerComputeContext()
	cat(paste("Connection String Tested:\n",
		"  ", sqlCompute@connectionString, "\n"))
    rxuEnsureSameClientServerCollate()    
}

"test.SqlServer.HPA.rxCrossTabs" <- function()
{ 
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCrossTabs", 
	    analysisParams = list(
            formula = ArrDelay10~DayOfWeek, 
            transforms = substitute(list(ArrDelay10 = ArrDelay)),
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = c("sums",  "counts", "chisquare")
        )
}


"test.SqlServer.HPA.rxCube" <- function()
{
    
    rxuTestAnalysisFunctionWithAirDemo( 
		analysisName = "rxCube", 
		analysisParams = list(
            formula = ArrDelay10~DayOfWeek, 
            transforms = substitute(list(ArrDelay10 = ArrDelay)),
            reportProgress = 0
            ),
		testValues = c("ArrDelay10",  "Counts")
        )    
}
    

"test.SqlServer.HPA.rxLinMod" <- function()
{ 
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLinMod", 
	    analysisParams = list(
            formula = ArrDelay~DayOfWeek, 
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = c("coefficients", "residual.squares", "condition.number",
            "rank", "coef.std.error", "coef.t.value", "df"  )
        )          
}

"test.SqlServer.HPA.rxLinMod.stepwise" <- function()
{
    direction <- "both"    
    scope <- NULL
    steps <- 1000
    scale <- 0
    k <- 2
    test <- NULL       
    
    varSel <- list( method = "stepwise", maxSteps = 100, scale = 0, k = 2, matchRStep = FALSE)
    
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLinMod", 
	    analysisParams = list(
            formula = ArrDelay~DayOfWeek + CRSDepTime, 
            variableSelection = substitute(varSel),
            reportProgress = 0
            ),
	    testValues = c("coefficients", "residual.squares", "condition.number",
            "rank", "coef.std.error", "coef.t.value", "df"  )
        )         
}

"test.SqlServer.HPA.rxLinMod.args.formula.F.high.low" <- function()
{

    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLinMod", 
	    analysisParams = list(
            formula = ArrDelay~DayOfWeek + F(CRSDepTime, low = 0, high = 24), 
            reportProgress = 0
            ),
	    testValues = c("coefficients", "residual.squares", "condition.number",
            "rank", "coef.std.error", "coef.t.value", "df"  )
        )     
}

"test.SqlServer.HPA.rxCovCor" <- function()
{
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCovCor", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime10, 
            transforms = substitute(list(CRSDepTime10 = CRSDepTime*10)),
            reportProgress = 0
            ),
	    testValues = c("CovCor", "StdDevs", "Means")
        ) 

}

"test.SqlServer.HPA.rxCov" <- function()
{
    
     rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCov", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime10, 
            transforms = substitute(list(CRSDepTime10 = CRSDepTime * 10)),
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = NULL
        )      
}

"test.SqlServer.HPA.rxCor" <- function()
{
    
     rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCor", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime, 
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = NULL
        )
}

"test.SqlServer.HPA.rxSSCP" <- function()
{
    
      rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxSSCP", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime, 
            reportProgress = 0
            ),
	    testValues = NULL
        )

}

"test.SqlServer.HPA.rxLogit" <- function()
{
     rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLogit", 
	    analysisParams = list(
            formula = Late~DayOfWeek, 
            transforms = substitute(list(Late = ArrDelay > 15)),
            maxIterations = 6,
            reportProgress = 0
            ),
	    testValues = c("coefficients", "condition.number", "rank", "coef.std.error",
            "coef.t.value", "total.squares", "df", "deviance", "aic")
        ) 
       
}


"test.SqlServer.HPA.rxGlm" <- function()
{
      rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxGlm", 
	    analysisParams = list(
            formula = ArrOnTime ~ CRSDepTime, 
            family = binomial(link = "probit"),
            transforms = substitute(list(ArrOnTime = ArrDelay < 2)),
            maxIterations = 6,
            reportProgress = 0
            ),
	    testValues = c("coefficients", "condition.number", "rank", "coef.std.error",
            "coef.t.value", "df", "deviance")
        ) 
    
}

"test.SqlServer.HPA.rxKmeans" <- function()
{
  
  testCenters <- matrix(c(13, 20, 7, 17, 10), ncol = 1)     
  rxuTestAnalysisFunctionWithAirDemo( 
    analysisName = "rxKmeans", 
    analysisParams = list(
        formula = ~CRSDepTime, 
        centers = substitute(testCenters),
        reportProgress = 0
        ),
    testValues = c("centers", "withinss", "size")
    )         
}

"test.SqlServer.HPA.rxKmeans.outFile" <- function()
{
	if (!getHaveWritePermission())
	{
		DEACTIVATED("Deactivated because test requires write permission.")
	}

   # Local computation
    testCenters <- matrix(c(13, 20, 7, 17, 10), ncol = 1)
    localOutFile <- tempfile(pattern = "rxKmeans", fileext = ".xdf")
    rxSetComputeContext( getLocalComputeContext() )
    cat("rxKmeans(local)\n");
    localKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoXdfLocalDS(), 
        outFile = localOutFile, centers = testCenters, 
        outColName = "X_rxCluster",  # Modified for ODBC data source types
        writeModelVars = TRUE, overwrite = TRUE)

    cat("rxSummary(local)\n");
    localSummary <- rxSummary(~., data = localOutFile)  
    file.remove(localOutFile)      
   
    # In-Database computation
    sqlCompute <- getSqlServerComputeContext()
    rxSetComputeContext( sqlCompute )
	
	# Create output data source
	tempTable <- "rxTempKmeansTest"
	if (rxSqlServerTableExists(tempTable)) rxSqlServerDropTable(tempTable)	
    sqlServerOutDS <- RxSqlServerData( table=tempTable, connectionString = sqlCompute@connectionString)
	# Run rxKmeans	
	startTime <- Sys.time()
	cat("rxKmeans(sql)\n");
	sqlKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoSqlServerDS(), 
        outFile = sqlServerOutDS, centers = testCenters, 
        writeModelVars = TRUE, overwrite = TRUE) 
	endTime <- Sys.time()
	print(endTime - startTime)
	# Compute summaries of output data
	cat("rxSummary(sql)\n");
	sqlSummary <- rxSummary(~., data = sqlServerOutDS)
	rxSqlServerDropTable(tempTable)
	# Compare results
	checkEquals(localSummary$sDataFrame, sqlSummary$sDataFrame, tolerance=1e-6)   
}

"test.SqlServer.HPA.rxDTree" <- function()
{  
 
	#rxSetComputeContext( getLocalComputeContext() )
	#myDataSource <- getAirDemoXdfLocalDS()
    #localTree <- rxDTree(ArrDelay~ CRSDepTime, data = myDataSource,
         #maxDepth = 2,  xVal = 0, minSplit = 775,
	     #maxNumBins = 101, allowDiskWrite = FALSE, reportProgress = 0)	
	#
    #rxSetComputeContext( getSqlServerComputeContext() )
	#myDataSource <- getAirDemoSqlServerDS()
    #SqlServerTree <- rxDTree(ArrDelay~ CRSDepTime, data = myDataSource,
         #maxDepth = 2,  xVal = 0, minSplit = 775,
	     #maxNumBins = 101, allowDiskWrite = FALSE, reportProgress = 0)	
	
  rxuTestAnalysisFunctionWithAirDemo( 
    analysisName = "rxDTree", 
    analysisParams = list(
        formula = ArrDelay~CRSDepTime, 
        maxDepth       = 22,
        xVal           = 0,
        minSplit       = 775,
        maxNumBins     = 101,
		allowDiskWrite = FALSE,
        reportProgress = 0
        ),
    testValues = c("frame", "where", "cptable")
    )    
                         
}  

"test.SqlServer.HPA.rxDForest" <- function()
{  
	
    # Compute local forest
	rxSetComputeContext( getLocalComputeContext() )
    myDataSource <-  getAirDemoXdfLocalDS()
 	
    localForest <- rxDForest(ArrDelay~CRSDepTime, data = myDataSource,
        computeOobError = -1, nTree = 2, maxDepth = 2, numThreads = 1, 
		batchLength = 1, reportProgress = 0) 
		
	# Compute SqlServer forest
    rxSetComputeContext( getSqlServerComputeContext() )
	myDataSource <- getAirDemoSqlServerDS()
    sqlServerForest <- rxDForest(ArrDelay~CRSDepTime, data = myDataSource,
        computeOobError = -1, nTree = 2, maxDepth = 2, numThreads = 1, 
		batchLength = 1, reportProgress = 0) 	
	
    checkEquals( sort(class( sqlServerForest )), "rxDForest" )

	# NOTE: the forests will be different for local and distributed 
	# as the RNG used in local and distributed cases are different.

	checkEquals(length(sqlServerForest$forest), length(localForest$forest))		
                  
}  

"test.SqlServer.HPA.rxBTrees" <- function()
{
  
	form <- Late~CRSDepTime + DayOfWeek
    maxDepth <- 10
    xVal  <- 0
    minSplit <- 775
    maxNumBins <- 775
	allowDiskWrite <- FALSE
	lossFunction <- "bernoulli"
	nTree <- 5
	learningRate <- 0.1    
	reportProgress <- 2
    verbose <- 0
	seed <- 10
    ###################################################################
	## local
    rxSetComputeContext( getLocalComputeContext() )

	localResult <- rxBTrees(form, data = getAirDemoXdfLocalDS(),
		transforms = substitute(list(Late = ArrDelay > 15)),
        minSplit = minSplit, maxDepth = maxDepth,
        nTree = nTree, seed = seed,
        lossFunction = lossFunction, learningRate = learningRate,
        maxNumBins = maxNumBins, 
        reportProgress = reportProgress, verbose = verbose)
	
    rxSetComputeContext( getSqlServerComputeContext() )
	sqlServerResult <- rxBTrees(form, data = getAirDemoSqlServerDS(),
		transforms = substitute(list(Late = ArrDelay > 15)),
        minSplit = minSplit, maxDepth = maxDepth,
        nTree = nTree, seed = seed,
        lossFunction = lossFunction, learningRate = learningRate,
        maxNumBins = maxNumBins, 
        reportProgress = reportProgress, verbose = verbose)

	# NOTE: the forests will be different for local and distributed 
	# as the RNG used in local and distributed cases are different and data may be in different order
    attrs <- c("ntree", "mtry", "type", "cutoff", "formula")   #"forest", "oob.err", "confusion", "params", "call", 
    checkEquals(localResult[attrs], sqlServerResult[attrs])
	checkEquals(length(localResult$forest), length(sqlServerResult$forest))
    checkTrue(!any(sapply(sqlServerResult$oob.err$oob.err, function(x) {is.null(x) || is.na(x)})))
}

#############################################################################################
#
# Additional HPA Functions
#
#############################################################################################

"test.SqlServer.HPA.wrapper.rxQuantile" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localQuantile <- rxQuantile("ArrDelay", data = getAirDemoXdfLocalDS()) 
    
    if (getTestOdbc())
    {    
        odbcQuantile <- rxQuantile("ArrDelay", data = getAirDemoSqlServerDS())
        checkEquals(localQuantile, odbcQuantile)
    }
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlServerQuantile <- rxQuantile("ArrDelay", data = getAirDemoSqlServerDS())
    
    checkEquals(localQuantile, sqlServerQuantile)
    
    # Try with low/high value specified in colInfo
    sqlServerDS <- getAirDemoSqlServerDS()
    sqlServerDS@colInfo <- list(ArrDelay = list(low = -86, high = 1490))
    sqlServerQuantile1 <- rxQuantile("ArrDelay", data = sqlServerDS)
    checkEquals(localQuantile, sqlServerQuantile1)
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.SqlServer.HPA.wrapper.rxLorenz" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    
	localLorenz <- rxLorenz(orderVarName = "CRSDepTime", data = getAirDemoXdfLocalDS())
	
	lorenzPlot <- plot(localLorenz, equalityWidth = 1, 
		title = "Lorenz Curve for CRSDepTime Computed Locally",
		equalityColor = "black", equalityStyle = "longdash", lineWidth = 3)
	
	# Compute Gini
	localGini <- rxGini(localLorenz)

    if (getTestOdbc())
    {    
        odbcLorenz <- rxLorenz(orderVarName = "CRSDepTime", data = getAirDemoSqlServerDS())
        odbcGini <- rxGini(odbcLorenz)
        checkEquals(localLorenz, odbcLorenz)
        checkEquals(localGini, odbcGini)
    }
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlLorenz <- rxLorenz(orderVarName = "CRSDepTime", data = getAirDemoSqlServerDS())
    sqlGini <- rxGini(sqlLorenz)
	checkEquals(localLorenz, sqlLorenz)
    checkEquals(localGini, sqlGini)
    
	lorenzPlot <- plot(localLorenz, equalityWidth = 1, 
		title = "Lorenz Curve for CRSDepTime Computed In-Database",
		equalityColor = "black", equalityStyle = "longdash", lineWidth = 3)    
    
    # Try with low/high value specified in colInfo
    sqlServerDS <- getAirDemoSqlServerDS()
    sqlServerDS@colInfo <- list(CRSDepTime = list(low = 0.0167, high = 23.9833))
    tdLorenz2 <- rxLorenz(orderVarName = "CRSDepTime", data = sqlServerDS)
    tdGini2 <- rxGini(tdLorenz2)
    checkEquals(localLorenz, tdLorenz2)
    checkEquals(localGini, tdGini2) 
    rxSetComputeContext( getLocalComputeContext() )
}

"test.SqlServer.HPA.wrapper.rxRoc" <- function()
{
	if (!getHaveWritePermission())
	{
		DEACTIVATED("Deactivated because test requires write permission.")
	}	
	rxSetComputeContext( getLocalComputeContext() )
    
	localLogit <- rxLogit( formula = Late~DayOfWeek, 
            transforms = list(Late = ArrDelay > 15), data = getAirDemoXdfLocalDS())

	tempXdf <- tempfile(pattern = "rxTestRoc", fileext = ".xdf")
	localPredict <- rxPredict(localLogit, data = getAirDemoXdfLocalDS(), 
		outData = tempXdf, writeModelVars = TRUE, predVarNames = "LatePred")
	localRoc <- rxRoc(actualVarName = "Late", predVarNames = "LatePred", 
		data = localPredict)
	
	
	plot(localRoc, title = "Local ROC Curve")
	#localCurve <- rxRocCurve(actualVarName = "Late", predVarNames = "LatePred", 
		#data = localPredict)
	file.remove(tempXdf)
	  
	sqlCompute <- getSqlServerComputeContext()
    rxSetComputeContext( sqlCompute )
	
	sqlLogit <- rxLogit( formula = Late~DayOfWeek, 
            transforms = list(Late = ArrDelay > 15), data = getAirDemoSqlServerDS())

	tempTableName <- "rxRocTemp"
	if (rxSqlServerTableExists(tempTableName)) rxSqlServerDropTable(tempTableName)
	tempSqlDS <- RxSqlServerData(table = tempTableName, connectionString = sqlCompute@connectionString)
	sqlPredict <- rxPredict(sqlLogit, data = getAirDemoSqlServerDS(), 
		outData = tempSqlDS, writeModelVars = TRUE, predVarNames = "LatePred", overwrite = TRUE)
	
    tempSqlDS <- RxSqlServerData(table = tempTableName, connectionString = sqlCompute@connectionString,
        colClasses = c("Late" = "logical"))
	sqlRoc <- rxRoc(actualVarName = "Late", predVarNames = "LatePred", 
		data = tempSqlDS)
	plot(sqlRoc, title = "ROC Curve Computed In-Database")
	rxSqlServerDropTable(tempTableName)
		
    checkEquals(sqlRoc, localRoc)
    rxSetComputeContext( getLocalComputeContext() )
}

"test.SqlServer.HPA.wrapper.rxHistogram.data.factor" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~DayOfWeek, data = getAirDemoXdfLocalDS(), 
        rowSelection = ArrDelay > 0, title = "Local Histogram: Factor Data" )
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlServerDS <- getAirDemoSqlServerDS()
    sqlServerHistogram <- rxHistogram(~DayOfWeek, data = sqlServerDS, 
        rowSelection = ArrDelay > 0, title = "Histogram Computed In-Database: Factor Data" )
    
    if ("lattice" %in% .packages())
    {

		checkIdentical(localHistogram$panel.args, sqlServerHistogram$panel.args)
	}
	else
	{
		checkIdentical(localHistogram, sqlServerHistogram)
	}
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.SqlServer.HPA.wrapper.rxHistogram.data.continuous" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~ArrDelay, data = getAirDemoXdfLocalDS(), 
        rowSelection = ArrDelay > 0, title = "Local Histogram: Continuous Data" )
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlServerHistogram <- rxHistogram(~ArrDelay, data = getAirDemoSqlServerDS(), 
        rowSelection = ArrDelay > 0, title = "Histogram Computed In-Database: Continuous Data" )
    
    checkIdentical(localHistogram$panel.args, sqlServerHistogram$panel.args)
    checkIdentical(localHistogram$call, sqlServerHistogram$call)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.SqlServer.HPA.wrapper.rxHistogram.data.integers" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~DepTimeInt, data = getAirDemoXdfLocalDS(), 
        transforms = list(DepTimeInt = as.integer(CRSDepTime)), title = "Local Histogram: Integers" )
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlDataSource <- getAirDemoSqlServerDS()
    sqlDataSource@colInfo[["CRSDepTime"]] <- list(type = "integer", low = 0, high = 23)
    sqlServerHistogram <- rxHistogram(~CRSDepTime, data = sqlDataSource, 
         title = "Histogram Computed In-Database: Integers" )
    
    checkIdentical(localHistogram$panel.args, sqlServerHistogram$panel.args)
    checkIdentical(localHistogram$call, sqlServerHistogram$call)
   
    rxSetComputeContext( getLocalComputeContext() )
}


#############################################################################################
#
# Tests for compute context helper functions
#
#############################################################################################
    
#############################################################################################
# Tests for getting data source information
#############################################################################################

"test.SqlServer.dataSource.rxGetInfo" <- function()
{
    
    rxSetComputeContext( getLocalComputeContext() )
    localInfo1 <- rxGetInfo( data = getAirDemoXdfLocalDS())
    localInfo2 <- rxGetInfo( data = getAirDemoXdfLocalDS(), getVarInfo = TRUE)
    localInfo3 <- rxGetInfo( data = getAirDemoXdfLocalDS(), getVarInfo = TRUE, numRows = 5)
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlServerInfo1 <- rxGetInfo(data = getAirDemoSqlServerDS())
    sqlServerInfo2 <- rxGetInfo( data = getAirDemoSqlServerDS(), getVarInfo = TRUE,
        computeInfo = TRUE )
    sqlServerInfo3 <- rxGetInfo( data = getAirDemoSqlServerDS(), getVarInfo = TRUE,
        computeInfo = TRUE, numRows = 5 )    
    
    checkEquals(localInfo2$numRows, sqlServerInfo2$numRows)
    checkEquals(localInfo2$numVars, sqlServerInfo2$numVars)  
    
    # Order of data will differ
    checkEquals(nrow(localInfo3$data), nrow(sqlServerInfo3$data))
    rxSetComputeContext( getLocalComputeContext() )
}


"test.SqlServer.dataSource.rxGetVarInfo" <- function()
{
    
    rxSetComputeContext( getLocalComputeContext() )
    localVarInfo <- rxGetVarInfo( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlServerVarInfo <- rxGetVarInfo(getAirDemoSqlServerDS() )
    
    checkEquals(names(localVarInfo), names(sqlServerVarInfo)[1:3])  
      
    rxSetComputeContext( getLocalComputeContext() )
    sqlServerDS <- getAirDemoSqlServerDS()
	
    # Test tested without pre-specifying factor levels in colInfo  
    sqlColInfo <- sqlServerDS@colInfo
    sqlColInfo[["DayOfWeek"]] <- list(type = "factor")
	sqlColInfo["ArrDelay"] <- NULL
	sqlColInfo["CRSDepTime"] <- NULL
    sqlServerDS@colInfo <- sqlColInfo
    sqlServerVarInfo <- rxGetVarInfo(sqlServerDS, computeInfo = TRUE ) 
    checkEquals(localVarInfo$ArrDelay, sqlServerVarInfo$ArrDelay) 
    checkEquals(localVarInfo$CRSDepTime$low, sqlServerVarInfo$CRSDepTime$low, tolerance = 1e-6 ) 
    checkEquals(localVarInfo$CRSDepTime$high, sqlServerVarInfo$CRSDepTime$high, tolerance = 1e-6) 
    checkEquals(sort(localVarInfo$DayOfWeek$levels), sort(sqlServerVarInfo$DayOfWeek$levels)) 
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.SqlServer.dataSource.rxGetVarNames" <- function()
{   
    rxSetComputeContext( getLocalComputeContext() )
    localVarNames <- rxGetVarNames( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlServerVarNames <- rxGetVarNames( getAirDemoSqlServerDS() )
    
    checkEquals(localVarNames, sqlServerVarNames[1:3])
    
    rxSetComputeContext( getLocalComputeContext() ) 
}


"test.SqlServer.extra.rxLinePlot" <- function()
{    
    rxSetComputeContext( getLocalComputeContext() )
    localLinePlot <- rxLinePlot(ArrDelay~CRSDepTime, data = getAirDemoXdfLocalDS(), 
         rowSelection = ArrDelay > 60, type = "p", title = "Scatter Plot: Local Data" )
    
    rxSetComputeContext( getSqlServerComputeContext() )
    sqlServerLinePlot <- rxLinePlot(ArrDelay~CRSDepTime, data = getAirDemoSqlServerDS(), 
        rowSelection = ArrDelay > 60, type = "p", title = "Scatter Plot: SqlServer Data" )
    
    checkEquals(sort(localLinePlot$panel.args[[1]]$x), sort(sqlServerLinePlot$panel.args[[1]]$x))
    checkEquals(sort(localLinePlot$panel.args[[1]]$y), sort(sqlServerLinePlot$panel.args[[1]]$y))
   
    rxSetComputeContext( getLocalComputeContext() )
}


#############################################################################################
# Tests for import read
#############################################################################################


"test.SqlServer.dataSource.rxImport.to.data.frame" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localDataFrame <- rxImport( getAirDemoSqlServerDS() )
    localDataFrameSum <- rxSummary(~., data = localDataFrame)
    
    rxSetComputeContext( getSqlServerComputeContext() )
    # Note: this uses a local compute context under the hood
    sqlServerDataFrame <- rxImport( inData = getAirDemoSqlServerDS() )  
    
    rxSetComputeContext( getLocalComputeContext() )
    sqlServerDataFrameSum <- rxSummary(~., data = sqlServerDataFrame)
    
    checkEquals(localDataFrameSum$sDataFrame, sqlServerDataFrameSum$sDataFrame)
    checkEquals(localDataFrameSum$categorical, sqlServerDataFrameSum$categorical)    
   
}
#############################################################################################
# Tests for data step
#############################################################################################

"test.SqlServer.rxDataStep.aggregate" <- function()
{
	if (!getHaveWritePermission())
	{
		DEACTIVATED("Deactivated because test requires write permission.")
	}	
	ProcessChunk <- function( dataList)
	{	
		# Process Data 
		chunkTable <- table(as.data.frame(dataList))
		# Convert table to data frame with single row
		varNames <- names(chunkTable)
		  varValues <- as.vector(chunkTable)
		  dim(varValues) <- c(1, length(varNames))
		  chunkDF <- as.data.frame(varValues)
		  names(chunkDF) <- varNames
		  # Return the data frame 
		return( chunkDF )
	}
		
	sqlCompute <- getSqlServerComputeContext()
    rxSetComputeContext( sqlCompute )
			
	inDataSource <- getAirDemoSqlServerDS(specifyLevels = TRUE)
	currentSqlQuery <- "select DayOfWeek from AirlineDemoSmall"
	#inDataSource <- RxSqlServerData(sqlQuery = currentSqlQuery, 
		#colInfo = list(DayOfWeek = list(type = "factor", levels = as.character(1:7))))
	inDataSource@sqlQuery = currentSqlQuery
	inDataSource@table <- NULL
	
	iroDataSource = RxSqlServerData(table = "iroResults", connectionString = sqlCompute@connectionString)
	if (rxSqlServerTableExists("iroResults")) 
       rxSqlServerDropTable("iroResults")
		
        
	rxDataStep( inData = inDataSource, outFile = iroDataSource,
            varsToKeep = "DayOfWeek",
			transformFunc = ProcessChunk,
			reportProgress = 0, overwrite = TRUE)

	iroResults <- rxImport(iroDataSource)

	finalResults <- colSums(iroResults) 
	checkEquals(as.vector(finalResults) ,c(97975, 77725, 78875, 81304, 82987, 86159, 94975)) 
	rxSqlServerDropTable("iroResults")
	
	rxSetComputeContext( getLocalComputeContext() )
}


"test.SqlServer.rxDataStep.new.table" <- function()
{
	if (!getHaveWritePermission())
	{
		DEACTIVATED("Deactivated because test requires write permission.")
	}
    rxSetComputeContext( getLocalComputeContext() )
	localDS <- getAirDemoXdfLocalDS()
	localTempOut <- tempfile(pattern = "rxTestDataStep",  fileext = ".xdf")
	rxDataStep(inData = localDS, outFile = localTempOut, 
		transforms = list(ArrDelay10 = ArrDelay*10), rowSelection = ArrDelay > 60,
		overwrite = TRUE)
	localSummary <- rxSummary(~., localTempOut)
	file.remove(localTempOut)
	
    # In-Database
    sqlComputeContext <- getSqlServerComputeContext()
    rxSetComputeContext( sqlComputeContext )
	sqlServerDS <- getAirDemoSqlServerDS()
	
	tempTable <- "rxTempTestDataStep"
	if (rxSqlServerTableExists(tempTable)) rxSqlServerDropTable(tempTable)
	sqlServerTempOut <- RxSqlServerData( table = tempTable, connectionString = sqlComputeContext@connectionString)
	dsOut <- rxDataStep(inData = sqlServerDS, outFile = sqlServerTempOut, 
		transforms = list(ArrDelay10 = ArrDelay*10), rowSelection = ArrDelay > 60,
		overwrite = TRUE)
	# Note: factors will be written as character data
	sqlServerTempOut <- RxDataSource(dataSource = sqlServerTempOut, colInfo = list( DayOfWeek =
		list(type = "factor", levels = c("Monday", "Tuesday", 
			"Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))))
	sqlServerSummary <- rxSummary(~., sqlServerTempOut)
	rxSqlServerDropTable(tempTable)
	
	rxSetComputeContext( getLocalComputeContext() )
	
	localStats <- localSummary$sDataFrame
	tdStats <- sqlServerSummary$sDataFrame
	row.names(tdStats) <- NULL

	checkEquals(localStats, tdStats, tolerance = 1e-6 )
	checkEquals(localSummary$categorical, sqlServerSummary$categorical)

}

#############################################################################################
# Tests for rxPredict
#############################################################################################
"test.SqlServer.HPA.rxPredict" <- function()
{
	if (!getHaveWritePermission())
	{
		DEACTIVATED("Deactivated because test requires write permission.")
	}
    rxSetComputeContext( getLocalComputeContext() )
    
	localInDS <- getAirDemoXdfLocalDS()
    localLinMod <- rxLinMod(ArrDelay~DayOfWeek + CRSDepTime, data = localInDS)
	localOutFile <- tempfile(pattern = "rxPredictTempTest", fileext = ".xdf")
    localPred <- rxPredict(localLinMod, data = localInDS, outData = localOutFile, 
        writeModelVars = TRUE, overwrite = TRUE)
    localSummary <- rxSummary(~., data = localOutFile)  
    file.remove(localOutFile) 
	
    # In-Database computation
    sqlCompute <- getSqlServerComputeContext()
    rxSetComputeContext( sqlCompute )
	
	sqlServerInDS <- getAirDemoSqlServerDS()
	sqlServerLinMod <- rxLinMod(ArrDelay~DayOfWeek + CRSDepTime, data = sqlServerInDS)	
	
	# Create output data source
	tempTable <- "rxTempPredictTest"
	if (rxSqlServerTableExists(tempTable)) rxSqlServerDropTable(tempTable)	
    sqlServerOutDS <- RxSqlServerData( table=tempTable, connectionString = sqlCompute@connectionString)
	
	# Compute predictions	
    sqlServerPred <- rxPredict(sqlServerLinMod, data = sqlServerInDS, outData = sqlServerOutDS, 
        writeModelVars = TRUE, overwrite = TRUE)	

	# Note: factor data is written out as character data
	sqlServerOutDS <- RxDataSource(dataSource = sqlServerOutDS, colInfo = list( DayOfWeek =
		list(type = "factor", levels = c("Monday", "Tuesday", 
			"Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))))	

    rxSqlServerTableExists(tempTable)
	sqlSummary <- rxSummary(~., data = sqlServerOutDS)
	rxSqlServerDropTable(tempTable)
	 rxSetComputeContext( getLocalComputeContext() )
	
	# Compare results
	#   order by Name and assign same rownames to assure matching rows during comparison
	localos <- localSummary$sDataFrame[order(localSummary$sDataFrame$Name), ]
	sqlos <- sqlSummary$sDataFrame[order(sqlSummary$sDataFrame$Name), ]
	rownames(localos) <- 1:nrow(localos)
	rownames(sqlos) <- 1:nrow(sqlos)

	checkEquals(localos, sqlos, tolerance=1e-6)
	checkEquals(localSummary$categorical, sqlSummary$categorical)
    
}
    
#############################################################################################
# rxExec Examples
#############################################################################################
"test.SqlServer.HPC.rxExec.elemArgs" <- function()
{
	# Pass in an argument
	countToX <- function(x)
	{
		return(1:x)
	}
	
	rxSetComputeContext( getLocalComputeContext() )
	argVals <- list(1,2,3)
    localExec <- rxExec(countToX, elemArgs = argVals)
	
	rxSetComputeContext( getSqlServerComputeContext() ) 
	sqlServerExec <- rxExec(countToX, elemArgs = argVals)
		
	localVecOut <- as.vector(unlist(localExec))
	sqlServerVecOut <- as.vector(unlist(sqlServerExec))
	checkEquals(localVecOut, sqlServerVecOut)
	
	rxSetComputeContext( getLocalComputeContext() )
	
	"testFuncSingleUntypedParam" <- function( singleArg )
	{
		print( singleArg )
		singleArg
	}
	rxSetComputeContext( getSqlServerComputeContext() ) 
	#sqlServerExec <- rxExec(testFuncSingleUntypedParam, elemArgs = NULL) # Doesn't work
	sqlServerExec <- rxExec( testFuncSingleUntypedParam, rxElemArg( list(NULL, NULL, NULL, NULL) ))
	checkEquals( 4L, length( sqlServerExec ) )
	checkTrue( is.null(unlist(sqlServerExec)) )	
	
	rxSetComputeContext( getSqlServerComputeContext() ) 
	testOutput <- rxExec(function(){ rnorm(5) }, timesToRun=1, taskChunkSize=5)
	checkEquals(1L, length(testOutput))
}


"test.SqlServer.HPC.rxExec.timesToRun" <- function()
{
	countToTen <- function()
	{
		return(1:10)
	}
	
	rxSetComputeContext( getLocalComputeContext() ) 
    localExec <- rxExec(countToTen, timesToRun = 5)
	
	rxSetComputeContext( getSqlServerComputeContext() ) 
	sqlServerExec <- rxExec(countToTen, timesToRun = 5)
	
	localVecOut <- as.vector(unlist(localExec))
	sqlServerVecOut <- as.vector(unlist(sqlServerExec))
	checkEquals(localVecOut, sqlServerVecOut)
	
	# Pass in an argument
	countToX <- function(x)
	{
		return(1:x)
	}
	
	upperVal <- 3
	rxSetComputeContext( getLocalComputeContext() )
    localExec <- rxExec(countToX, x=upperVal, timesToRun = 4)
	
	rxSetComputeContext( getSqlServerComputeContext() ) 
	sqlServerExec <- rxExec(countToX, x=upperVal, timesToRun=4)
		
	localVecOut <- as.vector(unlist(localExec))
	sqlServerVecOut <- as.vector(unlist(sqlServerExec))
	checkEquals(localVecOut, sqlServerVecOut)
	
	# Pass in vector of arguments
	
	timesToRun <- 5
	upperVal <- 1:timesToRun
	rxSetComputeContext( getLocalComputeContext() )
    localExec <- rxExec(countToX, rxElemArg(x=upperVal), timesToRun = timesToRun)
	
	rxSetComputeContext( getSqlServerComputeContext() ) 
    sqlServerExec <- NULL
	sqlServerExec <- rxExec(countToX, rxElemArg(x=upperVal), timesToRun=timesToRun)
      	
	rxSetComputeContext( getLocalComputeContext() ) 
	localVecOut <- as.vector(unlist(localExec))
	sqlServerVecOut <- as.vector(unlist(localExec))
	checkEquals(localVecOut, sqlServerVecOut)
	
	# Just rxElemArg
	
    timesToRun <- 5
	upperVal <- 1:timesToRun
	rxSetComputeContext( getLocalComputeContext() )
    localExec <- rxExec(countToX, rxElemArg(x=upperVal))
	
	rxSetComputeContext( getSqlServerComputeContext() ) 
	sqlServerExec <- rxExec(countToX, rxElemArg(x=upperVal))
	
	rxSetComputeContext( getLocalComputeContext() ) 
	localVecOut <- as.vector(unlist(localExec))
	sqlServerVecOut <- as.vector(unlist(localExec))
	checkEquals(localVecOut, sqlServerVecOut)	
	
}



"test.SqlServer.HPC.rxExec.taskChunkSize" <- function()
{
	rollDice <- function()
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
    rxSetComputeContext( getLocalComputeContext()) 
    localExec <- rxExec(rollDice, timesToRun = 5, taskChunkSize = 2)
	
	rxSetComputeContext( getSqlServerComputeContext() ) 
	sqlServerExec <- rxExec(rollDice, timesToRun = 5, taskChunkSize = 2)
	checkEquals(length(localExec), length(sqlServerExec))
	checkEquals(length(localExec[[1]]), length(sqlServerExec[[1]]))
	
	rxSetComputeContext( getLocalComputeContext() ) 
	
}	

"test.SqlServer.HPC.foreach" <- function()
{
	DEACTIVATED("'foreach' not supported in SQL Server.")
	rollDice <- function()
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
		
	library(doRSR)
    registerDoRSR()

	### Local computations   
    rxSetComputeContext( getLocalComputeContext() ) 
	    
	#localForeach <- foreach(i=1:1000, .options.rsr=list(chunkSize=200)) %dopar% rollDice()
   	localForeach <- foreach(i=1:10) %dopar% rollDice()
	localForeachTable <- table(unlist(localForeach))

	### In SqlServer
	rxSetComputeContext( getSqlServerComputeContext() ) 
	sqlForeach <- foreach(i=1:10) %dopar% rollDice()
	sqlForeachTable <- table(unlist(sqlForeach))
	checkEquals(sum(localForeachTable), 10)
	checkEquals(sum(sqlForeachTable), 10)
	#checkTrue(as.integer(sqlForeachTable["Win"]) > 350)
	#checkTrue(as.integer(sqlForeachTable["Loss"]) > 350)
	
	rxSetComputeContext( getLocalComputeContext() ) 
	
}	

#############################################################################################
# Tests for running arbitrary SQL statement
#############################################################################################

 "test.SqlServer.helper.rxSqlServerSql" <- function()
{
	DEACTIVATED("No function available to run a SQL statement.")
	sqlDataSource <- getAirDemoSqlServerDS()
		
	if (rxSqlServerTableExists("AirlineCopy", connectionString = sqlDataSource@connectionString)) 
		rxSqlServerDropTable("AirlineCopy", connectionString = sqlDataSource@connectionString)
	
	# Make a copy of the table
	sqlStatement <- "CREATE TABLE AirlineCopy"
	rxSqlServerSql(sqlStatement = sqlStatement, connectionString = sqlDataSource@connectionString)
	sqlStatement <- "select * into AirlineCopy from AirlineDemoSmall"
	sqlDataSourceNew <- RxSqlServerData(sqlQuery = "select * from AirlineCopy",
		connectionString = sqlDataSource@connectionString)
	
	rxSetComputeContext( getSqlServerComputeContext() )
	sumOutOld <- rxSummary(~ArrDelay + CRSDepTime, data = sqlDataSource)
	sumOutNew <- rxSummary(~ArrDelay + CRSDepTime, data = sqlDataSourceNew)
	checkEquals(sumOutOld$sDataFrame, sumOutNew$sDataFrame)
	
	# Delete the copied table
	rxSqlServerDropTable(table = "AirlineCopy", connectionString = sqlDataSource@connectionString)
	rxSetComputeContext( getLocalComputeContext() ) 
	
	res <- try(
		rxGetVarInfo(sqlDataSourceNew)
		)
	checkTrue(inherits(res,"try-error"))
	  
}

#############################################################################################
# Tests for running R scripts inside T-SQL (add SQL tests in SqlServerIOQ_Tests.sql first)
#############################################################################################
"rxuTestAnalysisFunctionWithTSQL" <- function(funName, captureLogging = FALSE, doPredict = FALSE)
{
    ## get the connectionString
    sqlCompute <- getSqlServerComputeContext()
    connectionString <- sqlCompute@connectionString
    
    ## extra the components from the connectionString
    connectionSubstr <- strsplit(connectionString, "=|;")[[1]]
    connection <- setNames(
        connectionSubstr[seq.int(from = 2, to = length(connectionSubstr), by = 2)],
        connectionSubstr[seq.int(from = 1, to = length(connectionSubstr), by = 2)])
    
    ## form the basic sqlcmd command line
    pathToSql <- gsub("/", "\\\\", system.file("demoScripts", package = "RevoScaleR")) #sqlcmd cannot handle forward slash in path
    #pathToSql <- file.path(Sys.getenv("RXSVNROOT"), "revoAnalytics/inst/demoScripts")   #for dev
    if ("Trusted_Connection" %in% names(connection))
    {
        sqlCmd <- sprintf(fmt = "sqlcmd -E -S \"%s\" -d \"%s\"", 
            connection["Server"], connection["Database"])
    } else {
        sqlCmd <- sprintf(fmt = "sqlcmd -U \"%s\" -P \"%s\" -S \"%s\" -d \"%s\"", 
        connection["Uid"], connection["Pwd"], connection["Server"], connection["Database"])
    }
    
    ## execute sqlcmd with specific sql script and scripting variables
    switch (funName,
        .setup = 
        {
            sqlScript <- file.path(pathToSql, "SqlServerIOQ_Tests_Setup.sql")    
            command <- sprintf(fmt = "%s -i \"%s\"", sqlCmd, sqlScript)
            logging <- system(command = command, intern = captureLogging)
            
            checkTrue(TRUE)
        },
        .teardown = 
        {
            sqlScript <- file.path(pathToSql, "SqlServerIOQ_Tests_Teardown.sql")
            command <- sprintf(fmt = "%s -i \"%s\"", sqlCmd, sqlScript)
            logging <- system(command = command, intern = captureLogging)
            
            checkTrue(TRUE)
        },
        {
            sqlScript <- file.path(pathToSql, "SqlServerIOQ_Tests.sql")
            command <- sprintf(fmt = "%s -i \"%s\" -v funname = %s connstr = \"%s\"", 
                sqlCmd, sqlScript, funName, connectionString)
            logging <- system(command = command, intern = captureLogging)
            
            sqlQuery  <- sprintf("SELECT * FROM SqlServerIOQ_Tests WHERE funname = '%s'", funName)
            sqlTable <- RxSqlServerData(sqlQuery = sqlQuery, connectionString = connectionString)
            sqlResult <- rxImport(inData = sqlTable)
            pass <- ifelse(nrow(sqlResult) > 0, sqlResult[["pass"]], FALSE)
            checkTrue(pass)
            
            if (doPredict) {
                sqlQuery  <- sprintf("SELECT * FROM SqlServerIOQ_Tests WHERE funname = '%s'", paste(funName, "rxPredict", sep = "_"))
                sqlTable <- RxSqlServerData(sqlQuery = sqlQuery, connectionString = connectionString)
                sqlResult <- rxImport(inData = sqlTable)
                pass <- ifelse(nrow(sqlResult) > 0, sqlResult[["pass"]], FALSE)
                checkTrue(pass)
            }
        }
    )
}

"test.SqlServer.TSQL.AAA.setup" <- function()
{ 
    sqlcmdInstalled <- try(system("sqlcmd -?", intern = TRUE))
    if (inherits(sqlcmdInstalled, "try-error")) stop(sqlcmdInstalled)
    
    rxuTestAnalysisFunctionWithTSQL(funName = ".setup", captureLogging = FALSE)
}

"test.SqlServer.TSQL.ZZZ.teardown" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = ".teardown", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxSummary" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxSummary", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxCrossTabs" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxCrossTabs", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxCube" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxCube", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxLinMod" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxLinMod", captureLogging = FALSE, doPredict = TRUE)
}

"test.SqlServer.TSQL.HPA.rxCovCor" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxCovCor", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxCov" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxCov", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxCor" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxCor", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxSSCP" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxSSCP", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxLogit" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxLogit", captureLogging = FALSE, doPredict = TRUE)
}

"test.SqlServer.TSQL.HPA.rxGlm" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxGlm", captureLogging = FALSE, doPredict = TRUE)
}

"test.SqlServer.TSQL.HPA.rxKmeans" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxKmeans", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxDTree" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxDTree", captureLogging = FALSE, doPredict = TRUE)
}

"test.SqlServer.TSQL.HPA.rxDForest" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxDForest", captureLogging = FALSE)
}

"test.SqlServer.TSQL.HPA.rxBTrees" <- function()
{ 
    rxuTestAnalysisFunctionWithTSQL(funName = "rxBTrees", captureLogging = FALSE)
}
