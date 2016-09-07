##############################################################################
# RevoScaleR SQL Server Getting Started Guide Examples
##############################################################################

# A preview of high-performance ‘big data’ analytics for SQL Server 2016 using 
# Microsoft R Server’s RevoScaleR package is included in Microsoft SQL Server 2016 
# Community Technology Preview 3 (CTP3).  This file contains the R code
# from the accompanying RevoScaleR SQL Server Getting Started Guide.

##############################################################################
# General setup/cleanup: Run this code before proceeding
##############################################################################

# Modify and uncomment the following code for your connection to SQL Server
# and other information about your SQL Serversetup:

# Note: for improved security, read connection string from a file, such as
# sqlConnString <- readLines("sqlConnString.txt")

#sqlConnString <- "Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester"	
#sqlShareDir = paste("c:\\AllShare\\", Sys.getenv("USERNAME"), sep="")

# Make sure that the local share dir exists:
if (!file.exists(sqlShareDir)) dir.create(sqlShareDir, recursive = TRUE)

# To follow the examples in this guide, you will need to have two
# csv data sets loaded into your database. Smaller versions with 10,000 observations
# are located  in the sample data directory of RevoScaleR. 
# Large data versions (with 10 million observations) can be downloaded from
# These data files are available from: http://packages.revolutionanalytics.com/datasets. 
# Modify the data locations and desired table names here if you are using
# the large versions of the data

ccFraudCsv <- file.path(rxGetOption("sampleDataDir"), "ccFraudSmall.csv")
ccScoreCsv <- file.path(rxGetOption("sampleDataDir"), "ccFraudScoreSmall.csv")


sqlFraudTable <- "ccFraudSmall"       # Use ccFraud10 for large data
sqlScoreTable <- "ccFraudScoreSmall"  # Use ccFraudScore10 for large data

sqlRowsPerRead = 5000

# If the data you are using has already been loaded into SQL, set this to FALSE:
loadSampleDataToSQL <- TRUE

# To use the big data sets, modify the following and uncomment
#sqlRowsPerRead = 250000
#ccFraudCsv <- "C:/ScaleR/Data/ccFraud.csv"
#ccFraudCsv <- "C:/ScaleR/Data/ccFraudScore.csv"
#sqlFraudTable <- "ccFraud10"
#sqlScoreTable <- "ccFraudScore10"  

##########################################################
# Using a SQL Server Data Source and Compute Context
##########################################################  
 
  
# Creating an RxSqlServerData Data Source
  
sqlFraudDS <- RxSqlServerData(connectionString = sqlConnString, 
     table = sqlFraudTable, rowsPerRead = sqlRowsPerRead)
sqlScoreDS <- RxSqlServerData(connectionString = sqlConnString, 
     table = sqlScoreTable, rowsPerRead = sqlRowsPerRead)
# Using rxDataStep to Load the Sample Data into SQL Server
if (loadSampleDataToSQL) {

inTextData <- RxTextData(file = ccFraudCsv, 
    colClasses = c(
    "custID" = "integer", "gender" = "integer", "state" = "integer",
    "cardholder" = "integer", "balance" = "integer", 
    "numTrans" = "integer",
    "numIntlTrans" = "integer", "creditLine" = "integer", 
    "fraudRisk" = "integer"))


rxDataStep(inData = inTextData, outFile = sqlFraudDS, overwrite = TRUE)
inTextData <- RxTextData(file = ccScoreCsv, 
    colClasses = c(
    "custID" = "integer", "gender" = "integer", "state" = "integer",
    "cardholder" = "integer", "balance" = "integer", 
    "numTrans" = "integer",
    "numIntlTrans" = "integer", "creditLine" = "integer"))

rxDataStep(inData = inTextData, sqlScoreDS, overwrite = TRUE)
}
  
# Extracting basic information about your data
  
rxGetVarInfo(data = sqlFraudDS)
  
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
sqlFraudDS <- RxSqlServerData(connectionString = sqlConnString, 
    table = sqlFraudTable, colInfo = ccColInfo, 
    rowsPerRead = sqlRowsPerRead)

rxGetVarInfo(data = sqlFraudDS)

sqlWait <- TRUE
sqlConsoleOutput <- FALSE
sqlCompute <- RxInSqlServer(
    connectionString = sqlConnString, 
    shareDir = sqlShareDir,
    wait = sqlWait,
    consoleOutput = sqlConsoleOutput)
  
# A Troubleshooting RxInSqlServer Compute Context
 
sqlComputeTrace <- RxInSqlServer(
    connectionString = sqlConnString, 
    shareDir = sqlShareDir,
    wait = sqlWait,
    consoleOutput = sqlConsoleOutput,
    traceEnabled = TRUE,
    traceLevel = 7)

##########################################################
# High-Performance In-Database Analytics in SQL Server
##########################################################  
  
# Compute summary statistics in SQL Server
sumStatsStart <- Sys.time()
  
# Set the compute context to compute in SQL Server
rxSetComputeContext(sqlCompute)
sumOut <- rxSummary(formula = ~gender + balance + numTrans + 
    numIntlTrans + creditLine, data = sqlFraudDS)
sumOut
# Set the compute context to compute locally
rxSetComputeContext ("local") 
sumStatsEnd <- Sys.time()
  
# Refining the RxSqlServerData data source
sumDF <- sumOut$sDataFrame
var <- sumDF$Name 
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
    balance = list(low = sumDF[var == "balance", "Min"], 
		high = sumDF[var == "balance", "Max"]),
    numTrans = list(low = sumDF[var == "numTrans", "Min"], 
		high = sumDF[var == "numTrans", "Max"]),
    numIntlTrans = list(low = sumDF[var == "numIntlTrans", "Min"], 
		high = sumDF[var == "numIntlTrans", "Max"]),
    creditLine = list(low = sumDF[var == "creditLine", "Min"], 
		high = sumDF[var == "creditLine", "Max"])
	)
sqlFraudDS <- RxSqlServerData(connectionString = sqlConnString, 
    table = sqlFraudTable, colInfo = ccColInfo, 
    rowsPerRead = sqlRowsPerRead)
# Visualizing your data using rxHistogram and rxCube
  
cubeStart <- Sys.time()
rxSetComputeContext(sqlCompute)

rxHistogram(~creditLine|gender, data = sqlFraudDS, 
histType = "Percent")
cube1 <- rxCube(fraudRisk~F(numTrans):F(numIntlTrans), 
data = sqlFraudDS)
cubePlot <- rxResultsDF(cube1)
levelplot(fraudRisk~numTrans*numIntlTrans, data = cubePlot)
cubeEnd <- Sys.time()
  
# Analyzing your data with rxLinMod
linModStart <- Sys.time()

  
linModObj <- rxLinMod(balance ~ gender + creditLine, 
data = sqlFraudDS)

summary(linModObj)
linModEnd <- Sys.time()


  
# Analyzing your data with rxLogit
logitStart <- Sys.time()
  
logitObj <- rxLogit(fraudRisk ~ state + gender + cardholder + balance + 
    numTrans + numIntlTrans + creditLine, data = sqlFraudDS, 
    dropFirst = TRUE)
summary(logitObj)
logitEnd <- Sys.time()

# Scoring a Data Set
scoreStart <- Sys.time()
sqlScoreDS <- RxSqlServerData(connectionString = sqlConnString, 
    table = sqlScoreTable, colInfo = ccColInfo, rowsPerRead = sqlRowsPerRead)
sqlServerOutDS <- RxSqlServerData(table = "ccScoreOutput", 
    connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead )
   
rxSetComputeContext(sqlCompute)
if (rxSqlServerTableExists("ccScoreOutput"))
    rxSqlServerDropTable("ccScoreOutput")
   
rxPredict(modelObject = logitObj, 
	data = sqlScoreDS,
	outData = sqlServerOutDS,
	predVarNames = "ccFraudLogitScore",
	type = "link",
	writeModelVars = TRUE,
	overwrite = TRUE)
if (rxSqlServerTableExists("ccScoreOutput"))
    rxSqlServerDropTable("ccScoreOutput")

rxPredict(modelObject = logitObj, 
	data = sqlScoreDS,
	outData = sqlServerOutDS,
	predVarNames = "ccFraudLogitScore",
	type = "link",
	writeModelVars = TRUE,
	extraVarsToWrite = "custID",
	overwrite = TRUE)
sqlMinMax <- RxSqlServerData(sqlQuery = paste(
      "SELECT MIN(ccFraudLogitScore) AS minVal,",
 	"MAX(ccFraudLogitScore) AS maxVal FROM ccScoreOutput"),
	connectionString = sqlConnString)
minMaxVals <- rxImport(sqlMinMax)
minMaxVals <- as.vector(unlist(minMaxVals))

sqlOutScoreDS <- RxSqlServerData(sqlQuery = 
	"Select ccFraudLogitScore FROM ccScoreOutput", 
      connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead,
 	colInfo = list(ccFraudLogitScore = list(
            low = floor(minMaxVals[1]), 
		high = ceiling(minMaxVals[2]))))

rxSetComputeContext(sqlCompute)
rxHistogram(~ccFraudLogitScore, data = sqlOutScoreDS)
scoreEnd <- Sys.time()		
  
##########################################################
# Using rxDataStep and rxImport
##########################################################  
dataStepStart <- Sys.time()  
sqlOutScoreDS <- RxSqlServerData(
    table =  "ccScoreOutput", 
    connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead )

sqlOutScoreDS2 <- RxSqlServerData(table = "ccScoreOutput2",
	connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead)

rxSetComputeContext(sqlCompute)
if (rxSqlServerTableExists("ccScoreOutput2"))
    rxSqlServerDropTable("ccScoreOutput2")

rxDataStep(inData = sqlOutScoreDS, outFile = sqlOutScoreDS2, 
	transforms = list(ccFraudProb = inv.logit(ccFraudLogitScore)), 
	transformPackages = "boot", overwrite = TRUE)
dataStepEnd <- Sys.time()
rxGetVarInfo(sqlOutScoreDS2)

 
# Using rxImport to Extract a Subsample
importStart <- Sys.time()

 
rxSetComputeContext("local")

sqlServerProbDS <- RxSqlServerData(
	sqlQuery = paste(
	"Select * FROM ccScoreOutput2",
 	"WHERE (ccFraudProb > .99)"),
	connectionString = sqlConnString)
highRisk <- rxImport(sqlServerProbDS)
orderedHighRisk <- highRisk[order(-highRisk$ccFraudProb),]
row.names(orderedHighRisk) <- NULL  # Reset row numbers
head(orderedHighRisk)
importEnd <- Sys.time()
  
# Using rxDataStep to Create a SQL Server Table
tableStart <- Sys.time()
rxSetComputeContext("local")
xdfAirDemo <- RxXdfData(file.path(rxGetOption("sampleDataDir"),
    "AirlineDemoSmall.xdf"))
rxGetVarInfo(xdfAirDemo)

sqlServerAirDemo <- RxSqlServerData(table = "AirDemoSmallTest",
	connectionString = sqlConnString)
if (rxSqlServerTableExists("AirDemoSmallTest",
    connectionString = sqlConnString))
    rxSqlServerDropTable("AirDemoSmallTest", 
    connectionString = sqlConnString)

rxDataStep(inData = xdfAirDemo, outFile = sqlServerAirDemo, 
	transforms = list(
		DayOfWeek = as.integer(DayOfWeek),
		rowNum = .rxStartRow : (.rxStartRow + .rxNumRows - 1)
	), overwrite = TRUE
)

rxSetComputeContext(sqlCompute)
SqlServerAirDemo <- RxSqlServerData(sqlQuery = 
	"SELECT * FROM AirDemoSmallTest",
	connectionString = sqlConnString,
      rowsPerRead = 50000,
	colInfo = list(DayOfWeek = list(type = "factor", 
		levels = as.character(1:7))))

rxSummary(~., data = sqlServerAirDemo)
tableEnd <- Sys.time()
  
# Performing Your Own Chunking Analysis
 chunkStart <- Sys.time() 

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
rxSetComputeContext( sqlCompute )
dayQuery <- 
"select DayOfWeek from AirDemoSmallTest"
inDataSource <- RxSqlServerData(sqlQuery = dayQuery, 
connectionString = sqlConnString,
 	rowsPerRead = 50000,
	colInfo = list(DayOfWeek = list(type = "factor", 
		levels = as.character(1:7))))
iroDataSource = RxSqlServerData(table = "iroResults", 
	connectionString = sqlConnString)
if (rxSqlServerTableExists(table = "iroResults", 
	connectionString = sqlConnString))
{
    rxSqlServerDropTable( table = "iroResults", 
	connectionString = sqlConnString)
}
rxDataStep( inData = inDataSource, outFile = iroDataSource,
        	transformFunc = ProcessChunk,
		overwrite = TRUE)

iroResults <- rxImport(iroDataSource)
finalResults <- colSums(iroResults)
finalResults
rxSqlServerDropTable( table = "iroResults", 
	connectionString = sqlConnString)
chunkEnd <- Sys.time()

##########################################################
# Analyzing Your Data Alongside SQL Server
##########################################################  
# Set the compute context to compute locally
rxSetComputeContext ("local") 
# Perform an rxSummary using a local compute context
localSumStart <- Sys.time()

   
sqlServerDS1 <- RxSqlServerData(connectionString = sqlConnString, 
    table = sqlFraudTable, colInfo = ccColInfo, rowsPerRead = 10000)

rxSummary(formula = ~gender + balance + numTrans + numIntlTrans +
creditLine, data = sqlServerDS1)
localSumEnd <- Sys.time()
localImportStart <- Sys.time()
statesToKeep <- sapply(c("CA", "OR", "WA"), grep, stateAbb)
statesToKeep
  
# Import of iata from a SQL Server Database
  
importQuery <- paste("SELECT gender,cardholder,balance,state FROM",
 	sqlFraudTable,
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
sqlServerImportDS <- RxSqlServerData(connectionString = sqlConnString, 
    	sqlQuery = importQuery, colInfo = importColInfo)

localDS <- rxImport(inData = sqlServerImportDS,
    	outFile = "ccFraudSub.xdf", 
    	overwrite = TRUE)
  
# Using the imported data
  
rxGetVarInfo(data = localDS)
rxSummary(~gender + cardholder + balance + state, data = localDS)	
localImportEnd <- Sys.time()
timeOut <- c(sumStats = difftime(sumStatsEnd, sumStatsStart, units = "secs"),
	        cube = difftime(cubeEnd, cubeStart,  units = "secs"),
			linMod = difftime(linModEnd, linModStart, units = "secs"),
			logit=  difftime(logitEnd, logitStart, units = "secs"),
			score = difftime(scoreEnd, scoreStart, units = "secs"),
			dataStep = difftime(dataStepEnd, dataStepStart, units = "secs"),
			import = difftime(importEnd, importStart, units = "secs"),
			table =  difftime(tableEnd, tableStart, units = "secs"),
			chunk = difftime(chunkEnd,  chunkStart, units = "secs"),
			localSum =  difftime(localSumEnd, localSumStart, units = "secs"),
			localImport =  difftime(localImportEnd, localImportStart, units = "secs"))

print("Timings in seconds:")
timeOut




