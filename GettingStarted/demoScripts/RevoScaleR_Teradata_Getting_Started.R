##############################################################################
# RevoScaleR RxTeradata Getting Started Guide Examples
##############################################################################

##############################################################################
# General setup/cleanup: Run this code before proceeding
##############################################################################

# To follow the examples in this guide, you will need the following 
# comma-delimited text data files  loaded into your Teradata database: 
#  ccFraud.csv as RevoTestDB.ccFraud10
#  ccFraudScore.csv as RevoTestDB.ccFraudScore10
# These data files are available from Revolution FTP site. 
# You can use the 'fastload' Teradata command to load the data sets into your 
# data base. 

# Modify and uncomment the following code for your connection to Teradata,
# and other information about your Teradata setup:

# Note: for improved security, read connection string from a file, such as
# tdConnString <- readLines("tdConnString.txt")

# tdConnString <- "DRIVER=Teradata;DBCNAME=machineNameOrIP
# DATABASE=RevoTestDB;UID=myUserID;PWD=myPassword;"

#tdShareDir = paste("c:\\AllShare\\", Sys.getenv("USERNAME"), sep="")
#tdRemoteShareDir = "/tmp/revoJobs"
#tdRevoPath = "/usr/lib64/Revo-7.4/R-3.1.3/lib64/R"

# Make sure that the local share dir exists:
if (!file.exists(tdShareDir)) dir.create(tdShareDir, recursive = TRUE)

# If your DATABASE is not named RevoTestDB, you will need
# to modify the SQL queries in the following code to your
# DATABASE name


##########################################################
# Using a Teradata Data Source and Compute Context
##########################################################  
 
  
# Creating an RxTeradata Data Source
  
tdQuery <- "SELECT * FROM ccFraud10"	
teradataDS <- RxTeradata(connectionString = tdConnString, 
sqlQuery = tdQuery, rowsPerRead = 50000)
  
# Extracting basic information about your data
  
rxGetVarInfo(data = teradataDS)
  
# Specifying column information in your data source
  
stateAbb <- c("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC",
    "DE", "FL", "GA", "HI","IA", "ID", "IL", "IN", "KS", "KY", "LA",
    "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NB", "NC", "ND",
    "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI","SC",
    "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY")


ccColInfo <- list(		
    gender = list(
	  type = "factor", 
        levels = c("1", "2"),
   	  newLevels = c("Male", "Female")),		
    cardholder = list(
	    type = "factor", 
	    levels = c("1", "2"),	
          newLevels = c("Principal", "Secondary")),
	state = list(
          type = "factor", 
          levels = as.character(1:51),
	    newLevels = stateAbb)
	)
teradataDS <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 50000)

rxGetVarInfo(data = teradataDS)

tdWait <- TRUE
tdConsoleOutput <- FALSE
tdCompute <- RxInTeradata(
    connectionString = tdConnString, 
    shareDir = tdShareDir,
    remoteShareDir = tdRemoteShareDir,
    revoPath = tdRevoPath,
    wait = tdWait,
    consoleOutput = tdConsoleOutput)
rxGetNodeInfo(tdCompute)
tdComputeTrace <- RxInTeradata(
    connectionString = tdConnString, 
    shareDir = tdShareDir,
    remoteShareDir = tdRemoteShareDir,
    revoPath = tdRevoPath,
    wait = tdWait,
    consoleOutput = tdConsoleOutput,
    traceEnabled = TRUE,
    traceLevel = 7)

##########################################################
# High-Performance In-Database Analytics in Teradata
##########################################################  
  
# Compute summary statistics in Teradata
  
# Set the compute context to compute in Teradata
rxSetComputeContext(tdCompute)
rxSummary(formula = ~gender + balance + numTrans + numIntlTrans +
creditLine, data = teradataDS)
# Set the compute context to compute locally
rxSetComputeContext ("local") 
  
# Refining the RxTeradata data source
  
ccColInfo <- list(		
    gender = list(
	  type = "factor", 
        levels = c("1", "2"),
   	  newLevels = c("Male", "Female")),		
    cardholder = list(
	    type = "factor", 
	    levels = c("1", "2"),	
          newLevels = c("Principal", "Secondary")),
	state = list(
          type = "factor", 
          levels = as.character(1:51),
	    newLevels = stateAbb),
      balance = list(low = 0, high = 41485),
      numTrans = list(low = 0, high = 100),
      numIntlTrans = list(low = 0, high = 60),
      creditLine = list(low = 1, high = 75)
	)
teradataDS <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 50000)
  
# Visualizing your data using rxHistogram and rxCube
  
rxSetComputeContext(tdCompute)

rxHistogram(~creditLine|gender, data = teradataDS, 
histType = "Percent")
cube1 <- rxCube(fraudRisk~F(numTrans):F(numIntlTrans), 
data = teradataDS)
cubePlot <- rxResultsDF(cube1)
levelplot(fraudRisk~numTrans*numIntlTrans, data = cubePlot)
  
# Analyzing your data with rxLinMod
  
linModObj <- rxLinMod(balance ~ gender + creditLine, 
data = teradataDS)

summary(linModObj)

  
# Analyzing your data with rxLogit
  
logitObj <- rxLogit(fraudRisk ~ state + gender + cardholder + balance + 
    numTrans + numIntlTrans + creditLine, data = teradataDS, 
    dropFirst = TRUE)
summary(logitObj)
  
# Scoring a Data Set
  
tdQuery <- "SELECT * FROM ccFraudScore10"
teradataScoreDS <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 50000)


teradataOutDS <- RxTeradata(table = "ccScoreOutput", 
    connectionString = tdConnString, rowsPerRead = 50000 )
   
rxSetComputeContext(tdCompute)
if (rxTeradataTableExists("ccScoreOutput"))
    rxTeradataDropTable("ccScoreOutput")
   
rxPredict(modelObject = logitObj, 
	data = teradataScoreDS,
	outData = teradataOutDS,
	predVarNames = "ccFraudLogitScore",
	type = "link",
	writeModelVars = TRUE,
	overwrite = TRUE)
if (rxTeradataTableExists("ccScoreOutput"))
    rxTeradataDropTable("ccScoreOutput")

rxPredict(modelObject = logitObj, 
	data = teradataScoreDS,
	outData = teradataOutDS,
	predVarNames = "ccFraudLogitScore",
	type = "link",
	writeModelVars = TRUE,
	extraVarsToWrite = "custID",
	overwrite = TRUE)
tdMinMax <- RxOdbcData(sqlQuery = paste(
      "SELECT MIN(ccFraudLogitScore),",
 	"MAX(ccFraudLogitScore) FROM ccScoreOutput"),
	connectionString = tdConnString)
minMaxVals <- rxImport(tdMinMax)
minMaxVals <- as.vector(unlist(minMaxVals))

teradataScoreDS <- RxTeradata(sqlQuery = 
	"Select ccFraudLogitScore FROM ccScoreOutput", 
      connectionString = tdConnString, rowsPerRead = 50000,
 	colInfo = list(ccFraudLogitScore = list(
            low = floor(minMaxVals[1]), 
		high = ceiling(minMaxVals[2]))))

rxSetComputeContext(tdCompute)
rxHistogram(~ccFraudLogitScore, data = teradataScoreDS)		
  
##########################################################
# Using rxDataStep and rxImport
##########################################################  
  
teradataScoreDS <- RxTeradata(
    sqlQuery =  "Select * FROM ccScoreOutput", 
    connectionString = tdConnString, rowsPerRead = 50000 )

teradataOutDS2 <- RxTeradata(table = " ccScoreOutput2",
	connectionString = tdConnString, rowsPerRead = 50000)

rxSetComputeContext(tdCompute)
if (rxTeradataTableExists("ccScoreOutput2"))
    rxTeradataDropTable("ccScoreOutput2")

rxDataStep(inData = teradataScoreDS, outFile = teradataOutDS2, 
	transforms = list(ccFraudProb = inv.logit(ccFraudLogitScore)), 
	transformPackages = "boot", overwrite = TRUE)
rxGetVarInfo(teradataOutDS2)

 
# Using rxImport to Extract a Subsample
 
rxSetComputeContext("local")

teradataProbDS <- RxTeradata(
	sqlQuery = paste(
	"Select * FROM ccScoreOutput2",
 	"WHERE (ccFraudProb > .99)"),
	connectionString = tdConnString)
highRisk <- rxImport(teradataProbDS)
orderedHighRisk <- highRisk[order(-highRisk$ccFraudProb),]
row.names(orderedHighRisk) <- NULL  # Reset row numbers
head(orderedHighRisk)
  
# Using rxDataStep to Create a Teradata Table

rxSetComputeContext("local")
xdfAirDemo <- RxXdfData(file.path(rxGetOption("sampleDataDir"),
    "AirlineDemoSmall.xdf"))
rxGetVarInfo(xdfAirDemo)

teradataAirDemo <- RxTeradata(table = " AirDemoSmallTest",
	connectionString = tdConnString)
if (rxTeradataTableExists("AirDemoSmallTest",
    connectionString = tdConnString))
    rxTeradataDropTable("AirDemoSmallTest", 
    connectionString = tdConnString)

rxDataStep(inData = xdfAirDemo, outFile = teradataAirDemo, 
	transforms = list(
		DayOfWeek = as.integer(DayOfWeek),
		rowNum = .rxStartRow : (.rxStartRow + .rxNumRows - 1)
	), overwrite = TRUE
)

rxSetComputeContext(tdCompute)
teradataAirDemo <- RxTeradata(sqlQuery = 
	"SELECT * FROM AirDemoSmallTest",
	connectionString = tdConnString,
      rowsPerRead = 50000,
	colInfo = list(DayOfWeek = list(type = "factor", 
		levels = as.character(1:7))))

rxSummary(~., data = teradataAirDemo)
  
# Performing Your Own Chunking Analysis
  

ProcessChunk <- function( dataList)
{	
    # Convert the input list to a data frame and 
    # call the 'table' function to compute the
    # contingency table 
	chunkTable <- table(as.data.frame(dataList))

	# Convert table output to data frame with single row
	varNames <- names(chunkTable)
	varValues <- as.vector(chunkTable)
	dim(varValues) <- c(1, length(varNames))
	chunkDF <- as.data.frame(varValues)
	names(chunkDF) <- varNames

	# Return the data frame, which has a single row
	return( chunkDF )
}
rxSetComputeContext( tdCompute )
tdQuery <- 
"select DayOfWeek from AirDemoSmallTest"
inDataSource <- RxTeradata(sqlQuery = tdQuery, 
 	rowsPerRead = 50000,
	colInfo = list(DayOfWeek = list(type = "factor", 
		levels = as.character(1:7))))
iroDataSource = RxTeradata(table = "iroResults", 
	connectionString = tdConnString)
if (rxTeradataTableExists(table = "iroResults", 
	connectionString = tdConnString))
{
    rxTeradataDropTable( table = "iroResults", 
	connectionString = tdConnString)
}
rxDataStep( inData = inDataSource, outFile = iroDataSource,
        	transformFunc = ProcessChunk,
		overwrite = TRUE)

iroResults <- rxImport(iroDataSource)
finalResults <- colSums(iroResults)
finalResults
rxTeradataDropTable( table = " iroResults", 
	connectionString = tdConnString)
   
   
##########################################################
# Using rxDataStep for By-Group Analyses
##########################################################  
  
# Creating a simulated data set
set.seed(10)
numObs <- 100000 
testData <- data.frame( 
	SKU = as.integer(runif(n = numObs, min = 1, max = 11)),
	INCOME = as.integer(runif(n = numObs, min = 10, max = 21)))
# SKU is also underlying coefficient
testData$SALES <- as.integer(testData$INCOME * testData$SKU +
25*rnorm(n = numObs))
  
# Uploading into a Teradata table
  
rxSetComputeContext("local") 
if (rxTeradataTableExists("sku_sales_100k", 
connectionString = tdConnString))
{
	rxTeradataDropTable("sku_sales_100k", 
connectionString = tdConnString)
}

teradataDS <- RxTeradata(table = "sku_sales_100k", 
connectionString = tdConnString)
rxDataStep(inData = testData, outFile = teradataDS)
  
# The By-Group Transformation Function
  
EstimateModel <- function(dataList) 
{	
      # Convert the input list to a data frame
	chunkDF <- as.data.frame(dataList)

      # Estimate a linear model on this chunk
      # Don't include the model frame in the model object
	model <- lm( SALES~INCOME, chunkDF, model=FALSE )

	# Print statements can be useful for debugging
	# print( model$coefficients )  

	# Remove unneeded parts of model object
	model$residuals <- NULL
	model$effects <- NULL
	model$fitted.values <- NULL
	model$assign <- NULL
	model$qr[[1]] <- NULL
	attr(model$terms, ".Environment") <- NULL
	
	# Convert the model to a character string
	modelString <- 
        rawToChar(serialize(model, connect=NULL, ascii=TRUE))
	
	# Pad to a fixed length
	modelString <- paste( modelString, 
        paste(rep("X", 2000 - nchar(modelString)), collapse = ""),
        sep="")

      # Create the entry for the results table
	resultDF <- data.frame(
        SKU = chunkDF[1,]$SKU, # Assumes SKU's all the same
        COEF = model$coefficients[2], # Slope
        MODEL = modelString )

	return( resultDF )
  } 
  
# Specifying the input by-group data source
  
tdQuery <- "SELECT * FROM sku_sales_100k"
partitionKeyword <- "PARTITION-WITH-VIEW"
partitionClause <- paste( partitionKeyword, "BY sku_sales_100k.SKU" )
inDataSource <- RxTeradata(sqlQuery = tdQuery, 
    tableOpClause = partitionClause, 
    rowsPerRead = 100000)
  
# Setting up the results data source
  
resultsDataSource = RxTeradata(table = "models", 
    connectionString = tdConnString )
rxOptions(transformPackages = c("stats"))
# Make sure compute context is in-database
rxSetComputeContext(tdCompute) 
  
# Run the by-group data step
  
rxDataStep( inData = inDataSource, outFile = resultsDataSource, 
    transformFunc = EstimateModel, reportProgress = 0, 
    overwrite = TRUE )
  
# Showing the estimated slopes
  
lmResults <- rxImport(resultsDataSource)
lmResults[order(lmResults$SKU),c("SKU", "COEF")]


  
# The Scoring transformation function
  
ScoreChunk <- function(dataList) 
{	
    chunkDF <- as.data.frame(dataList)
	
    # Extract the model string for the first observation
    # and convert back to model object
    # All observations in this chunk have the same SKU,
    # so they all share the same model
    modelString <- as.character(chunkDF[1,]$MODEL)
    model <- unserialize( charToRaw( modelString ) )

    resultDF <- data.frame( 
        SKU = chunkDF$SKU, 
        INCOME = chunkDF$INCOME, 
        # Use lm's predict method
        PREDICTED_SALES = predict(model, chunkDF) )

    return( resultDF ) 
 }
  
# Setting up the input data source for predictions
  
predictQuery <- paste("SELECT sku_sales_100k.SKU,",
 	"sku_sales_100k.INCOME, models.MODEL",
 	"FROM sku_sales_100k JOIN models ON",
 	"sku_sales_100k.SKU = models.SKU")
inDataSource <- RxTeradata(sqlQuery = predictQuery, 
connectionString = tdConnString,
	tableOpClause = partitionClause, rowsPerRead = 10000)
  
# Setting up the output data set for predictions
  
scoresDataSource = RxTeradata(table = "scores", 
    connectionString = tdConnString)
  
# Perform scoring in-database
  
rxSetComputeContext(tdCompute) 
rxDataStep( inData = inDataSource, outFile = scoresDataSource, 
    transformFunc = ScoreChunk, reportProgress = 0, 
    overwrite = TRUE )

  
# Extracting summary results
  
predSum <- rxImport( RxTeradata( 
	sqlQuery = paste("SELECT SKU, INCOME, MIN(PREDICTED_SALES),",
 		       "MAX(PREDICTED_SALES), COUNT(PREDICTED_SALES)",
 	             "FROM scores GROUP BY SKU, INCOME",
                   "ORDER BY SKU, INCOME"), 
	connectionString = tdConnString ) )
options(width = 120) # Set display width
predSum[1:15,]

   
# Clean-up
  
rxTeradataDropTable("models")
rxTeradataDropTable("scores")
rxSetComputeContext("local") 
  
##########################################################
# Performing Simulations In-Database
##########################################################  
 
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
 		else if (count == 1 && 
               (roll == 2 || roll == 3 || roll == 12)) 
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

   
# Calling rxExec
   	  
teradataExec <- rxExec(playCraps, timesToRun=1000, RNGseed="auto")
length(teradataExec)

table(unlist(teradataExec))	
##########################################################
# Analyzing Your Data Alongside Teradata
##########################################################  
# Set the compute context to compute locally
rxSetComputeContext ("local") 
tdQuery <- "SELECT * FROM ccFraudScore10"  
  
# Perform an rxSummary using a local compute context
  
teradataDS1 <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 500000)

rxSummary(formula = ~gender + balance + numTrans + numIntlTrans +
creditLine, data = teradataDS1)
statesToKeep <- sapply(c("CA", "OR", "WA"), grep, stateAbb)
statesToKeep
  
# Fast import of iata from a Teradata Database
  
importQuery <- paste("SELECT gender,cardholder,balance,state",
 	"FROM ccFraud10",
 	"WHERE (state = 5 OR state = 38 OR state = 48)")
importColInfo <- list(		
    gender = list(
	  type = "factor", 
        levels = c("1", "2"),
   	  newLevels = c("Male", "Female")),		
    cardholder = list(
	    type = "factor", 
	    levels = c("1", "2"),	
          newLevels = c("Principal", "Secondary")),
	state = list(
          type = "factor", 
          levels = as.character(statesToKeep),
	      newLevels = names(statesToKeep))
	)

rxSetComputeContext("local")
teradataImportDS <- RxTeradata(connectionString = tdConnString, 
    	sqlQuery = importQuery, colInfo = importColInfo)

localDS <- rxImport(inData = teradataImportDS,
    	outFile = "ccFraudSub.xdf", 
    	overwrite = TRUE)
  
# Using the imported data
  
rxGetVarInfo(data = localDS)
rxSummary(~gender + cardholder + balance + state, data = localDS)	


