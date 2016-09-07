
# Tutorials from RevoScaleR "Getting-Started" document

sampleDataDir <- rxGetOption("sampleDataDir")

myWd <- getwd()

# To import the AirlineDemoSmall.csv file into the .xdf data file format, use 
# rxImport as follows:
# note file compression 13.6MB for CSV -> 1.3MB
inputFile <- file.path(sampleDataDir, "AirlineDemoSmall.csv")

# airDS is an object rpresenting the xdf file
airDS <- rxImport(inData = inputFile, 
                  outFile = "ADS.xdf", 
                  missingValueString = "M", 
                  stringsAsFactors = TRUE,
                  overwrite = TRUE)

# explicitly specify DayOfWeek levels and overwrite xdf file
colInfo <- list(DayOfWeek = list(type = "factor", 
                                 levels = c("Monday", 
                                            "Tuesday", 
                                            "Wednesday", 
                                            "Thursday", 
                                            "Friday", 
                                            "Saturday", 
                                            "Sunday")))

airDS <- rxImport(inData = inputFile, outFile = "ADS.xdf", 
                  missingValueString = "M", 
                  colInfo = colInfo,  
                  overwrite = TRUE)  

# take a look at the data
nrow(airDS)
ncol(airDS) 
head(airDS)    
rxGetVarInfo(airDS)

# read an arbitrary chunk of the data set into a data frame 
# for further examination
myData <- rxReadXdf(airDS, numRows=10, startRow=100000)
myData

levels(myData$DayOfWeek)

# Use the rxSummary function to obtain descriptive statistics 
# for your .xdf data file
adsSummary <- rxSummary(~ArrDelay+CRSDepTime+DayOfWeek, data = airDS)
adsSummary
adsSummary <- summary( airDS )
adsSummary

# compute summary information by one or more categories by using interactions  
# of a numeric variable with a factor variable
rxSummary(~ArrDelay:DayOfWeek, data = airDS)

# histograms
options("device.ask.default" = T) 
rxHistogram(~ArrDelay, data = airDS) 
rxHistogram(~CRSDepTime, data = airDS) 
rxHistogram(~DayOfWeek, data = airDS)

# extract a subsample of the data file into a data frame in memory
myData <- rxDataStep(inData = airDS, 
                     rowSelection = ArrDelay > 240 & ArrDelay <= 300, 
                     varsToKeep = c("ArrDelay", "DayOfWeek"))
rxHistogram(~ArrDelay, data = myData)

# Use the rxLinMod function to fit a linear model using your ADS.xdf file
arrDelayLm1 <- rxLinMod(ArrDelay ~ DayOfWeek, data = airDS)
summary(arrDelayLm1)

# if CUBE = TRUE, regression is done using a partitioned inverse, 
# which may be faster and use less memory than standard regression computation
arrDelayLm2 <- rxLinMod(ArrDelay ~ DayOfWeek, data = airDS, cube = TRUE)
summary(arrDelayLm2)
