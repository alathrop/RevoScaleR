USE RevoTestDB;

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.fn_get_xdfname', 'FN') IS NOT NULL 
	DROP FUNCTION dbo.fn_get_xdfname;
GO
CREATE FUNCTION dbo.fn_get_xdfname
(
	@tblname AS VARCHAR(MAX)
)
RETURNS VARCHAR(MAX)
AS
BEGIN
	RETURN	   
		CASE @tblname
			WHEN 'AirlineDemoSmallR' THEN 'AirlineDemoSmall.xdf' 
			WHEN 'claims' THEN 'claims.xdf' 
			WHEN 'censusWorkers' THEN 'CensusWorkers.xdf' 
			WHEN 'mortDefaultSmall' THEN 'mortDefaultSmall.xdf' 
			ELSE @tblname + ' not supported.'
		END
END
GO

IF OBJECT_ID('dbo.fn_get_tblname', 'FN') IS NOT NULL 
	DROP FUNCTION dbo.fn_get_tblname;
GO
CREATE FUNCTION dbo.fn_get_tblname
(
	@xdfname AS VARCHAR(MAX)
)
RETURNS VARCHAR(MAX)
AS
BEGIN
	RETURN
		CASE @xdfname
			WHEN 'AirlineDemoSmall.xdf' THEN 'AirlineDemoSmallR' 
			WHEN 'claims.xdf' THEN 'claims' 
			WHEN 'CensusWorkers.xdf' THEN 'censusWorkers'
			WHEN 'mortDefaultSmall.xdf' THEN 'mortDefaultSmall' 
			ELSE @xdfname + ' not supported.'
		END
END
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.sp_exec_rre_fun', 'P') IS NOT NULL 
	DROP PROC dbo.sp_exec_rre_fun;
GO
CREATE PROC dbo.sp_exec_rre_fun
(
	@funname AS VARCHAR(MAX)
	, @funargs AS VARCHAR(MAX)
	, @fundata AS VARCHAR(MAX)
	, @connstr AS VARCHAR(MAX) = NULL
	, @isdistr AS BIT = 1
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @sp_execute_external_script AS VARCHAR(MAX) = ' sp_execute_external_script ';
	DECLARE @language AS VARCHAR(MAX) = ' @language = N''R'' ';
	DECLARE @script AS VARCHAR(MAX) = ', @script = N''
		#rxOptions(traceLevel = 7)
		funargs <- eval(parse(text = paste("list(", funargs, ")")))

		if (exists("connstr")) {
			distcc <- RxInSqlServer(connectionString = connstr)

			if (isdistr) 
			{
				runmode <- "distributed"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext(distcc)
				funargs$data <- RevoScaleR:::rxuCreateTestDataParameter(distcc, fileName = fundata)
			} 
			else 
			{
				runmode <- "local_odbc"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext("local")
				funargs$data <- RevoScaleR:::rxuCreateTestDataParameter(distcc, fileName = fundata)
			}
		} else {
			if (isdistr) 
			{
				runmode <- "local_df"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext("local")

				##TODO: we need to be able to specify the factor levels when importing.
				xdf <- file.path(rxGetOption("sampleDataDir"), fundata)
				varInfo <- rxGetVarInfo(xdf)
				factors <- names(which(sapply(inputDataSet, is.factor)))
				for (f in factors) {
					inputDataSet[[f]] <- factor(as.character(inputDataSet[[f]]), levels = varInfo[[f]][["levels"]])
				}

				funargs$data <- inputDataSet
			} 
			else 
			{
				runmode <- "local_xdf"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext("local")
				funargs$data <- file.path(rxGetOption("sampleDataDir"), fundata)
			}
		}

		model <- do.call(funname, funargs)
		#print(model)

		##TODO: the size of serialization of params$env of a model that involoves transforms is very big
		if (is.list(model) && !is.null(model$params)) model$params <- NULL

		##TODO: need to implement varchar to varbinary conversion in SQL
		#outputDataSet <- data.frame(mode = runmode, model = serialize(model, conn = NULL))	#failed

		myRawToChar <- function(myRaw, collapse = " ")
		{
			paste(myRaw, collapse = collapse)
		}
		outputDataSet <- data.frame(mode = runmode, model = myRawToChar(serialize(model, conn = NULL), collapse = " "))
		'' ';

	DECLARE @isfile AS BIT = patindex('%.%', @fundata);
	DECLARE @input_data_1 AS VARCHAR(MAX) = ', @input_data_1 = N''select * from ' + 
		IIF(@isfile = 1, dbo.fn_get_tblname(@fundata), @fundata) + ''' ';
	DECLARE @input_data_1_name AS VARCHAR(MAX) = ', @input_data_1_name = N''inputDataSet'' ';
	DECLARE @output_data_1_name AS VARCHAR(MAX) = ', @output_data_1_name = N''outputDataSet'' ';

	DECLARE @params AS VARCHAR(MAX) = ', @params = N''@funname varchar(max), @funargs varchar(max), @fundata varchar(max)' + 
	    IIF(@connstr IS NULL, '' , ', @connstr varchar(max)') + ', @isdistr BIT' + ''' ';
	DECLARE @func AS VARCHAR(MAX) = ', @funname = ''' + @funname + ''' ';
	DECLARE @args AS VARCHAR(MAX) = ', @funargs = ''' + @funargs + ''' ';
	DECLARE @data AS VARCHAR(MAX) = ', @fundata = ''' + IIF(@isfile = 1, @fundata, dbo.fn_get_xdfname( @fundata )) + ''' ';
	DECLARE @cstr AS VARCHAR(MAX) = ', @connstr = ''' + @connstr + ''' ';
	DECLARE @dist AS VARCHAR(MAX) = ', @isdistr = ' + CAST(@isdistr AS VARCHAR) + ' ';

	--TODO: need to keep a better separation between SQL and R and let outputDataSet decide the table schema if possible
	--DECLARE @result AS VARCHAR(MAX) = ' WITH RESULT SETS ((mode VARCHAR(MAX), ' + @funname + '_model VARBINARY(MAX))) ';
	DECLARE @result AS VARCHAR(MAX) = ' WITH RESULT SETS ((runmode VARCHAR(MAX), model VARCHAR(MAX))) ';

	DECLARE @sql AS VARCHAR(MAX) = 
		@sp_execute_external_script + @language + @script 
		+ @input_data_1 + @input_data_1_name + @output_data_1_name 
		+ @params + @func + @args +  @data + COALESCE(@cstr, '') + @dist
		+ @result;

	--PRINT @sql;
	EXEC(@sql);
END
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.sp_exec_rre_fun_pred', 'P') IS NOT NULL 
	DROP PROC dbo.sp_exec_rre_fun_pred;
GO
CREATE PROC dbo.sp_exec_rre_fun_pred
(
	@funname AS VARCHAR(MAX)
	, @funargs AS VARCHAR(MAX)
	, @prdargs AS VARCHAR(MAX)
	, @fundata AS VARCHAR(MAX)
	, @connstr AS VARCHAR(MAX) = NULL
	, @isdistr AS BIT = 1
	, @testvalues AS VARCHAR(MAX)
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @sp_execute_external_script AS VARCHAR(MAX) = ' sp_execute_external_script ';
	DECLARE @language AS VARCHAR(MAX) = ' @language = N''R'' ';
	DECLARE @script AS VARCHAR(MAX) = ', @script = N''
		#rxOptions(traceLevel = 7)

		## modeling
		#TODO: should we import the models from the model sql table?
		funargs <- eval(parse(text = paste("list(", funargs, ")")))
		runmode <- "local_xdf"
		RevoScaleR:::rxRuntimeTrace(runmode)
		rxSetComputeContext("local")
		funargs$data <- file.path(rxGetOption("sampleDataDir"), fundata)
		model <- do.call(funname, funargs)

		## prediction
		prdname <- "rxPredict"
		prdargs <- eval(parse(text = paste("list(", prdargs, ")")))
		prdargs$modelObject <- model
		prddata <- paste(funname, "rxPredict2", sep = "_")

		if (exists("connstr")) {
			distcc <- RxInSqlServer(connectionString = connstr)
		
			if (isdistr) 
			{
				runmode <- "distributed"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext(distcc)
				prdargs$data <- RevoScaleR:::rxuCreateTestDataParameter(distcc, fileName = fundata)
				prdargs$outData <- RevoScaleR:::rxuCreateTestDataParameter(distcc, fileName = prddata)

				pred <- do.call(prdname, prdargs)
				outputDataSet <- rxImport(inData = pred)
			} 
			else 
			{
				runmode <- "local_odbc"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext("local")
				prdargs$data <- RevoScaleR:::rxuCreateTestDataParameter(distcc, fileName = fundata)
				prdargs$outData <- RevoScaleR:::rxuCreateTestDataParameter(distcc, fileName = prddata)

				pred <- do.call(prdname, prdargs)
				outputDataSet <- rxImport(inData = pred)
			}

			if (rxSqlServerTableExists(prddata, connectionString = connstr)) {
				rxSqlServerDropTable(table = prddata, connectionString = connstr)
			}
		} else {
			if (isdistr) 
			{
				runmode <- "local_df"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext("local")
		
				##TODO: we need to be able to specify the factor levels when importing.
				xdf <- file.path(rxGetOption("sampleDataDir"), fundata)
				varInfo <- rxGetVarInfo(xdf)
				factors <- names(which(sapply(inputDataSet, is.factor)))
				for (f in factors) {
					inputDataSet[[f]] <- factor(as.character(inputDataSet[[f]]), levels = varInfo[[f]][["levels"]])
				}
		
				prdargs$data <- inputDataSet
				outputDataSet <- do.call(prdname, prdargs)
			} else {
				runmode <- "local_xdf"
				RevoScaleR:::rxRuntimeTrace(runmode)
				rxSetComputeContext("local")
				prdargs$data <- file.path(rxGetOption("sampleDataDir"), fundata)
				prdargs$outData <- file.path(tempdir(), prddata)

				pred <- do.call(prdname, prdargs)
				outputDataSet <- rxDataStep(inData = pred)

				if(file.exists(prdargs$outData)) unlink(prdargs$outData)
			}
		}

		outputDataSet <- cbind(mode = runmode, outputDataSet)
		#print(sapply(outputDataSet, class))
		#print(outputDataSet)
		'' ';

	DECLARE @isfile AS BIT = patindex('%.%', @fundata);
	DECLARE @input_data_1 AS VARCHAR(MAX) = ', @input_data_1 = N''select * from ' + 
		IIF(@isfile = 1, dbo.fn_get_tblname(@fundata), @fundata) + ''' ';
	DECLARE @input_data_1_name AS VARCHAR(MAX) = ', @input_data_1_name = N''inputDataSet'' ';
	DECLARE @output_data_1_name AS VARCHAR(MAX) = ', @output_data_1_name = N''outputDataSet'' ';

	DECLARE @params AS VARCHAR(MAX) = ', @params = N''@funname varchar(max), @funargs varchar(max), @prdargs varchar(max), @fundata varchar(max)' + 
	    IIF(@connstr IS NULL, '' , ', @connstr varchar(max)') + ', @isdistr BIT' + ''' ';
	DECLARE @func AS VARCHAR(MAX) = ', @funname = ''' + @funname + ''' ';
	DECLARE @args AS VARCHAR(MAX) = ', @funargs = ''' + @funargs + ''' ';
	DECLARE @pargs AS VARCHAR(MAX) = ', @prdargs = ''' + @prdargs + ''' ';
	DECLARE @data AS VARCHAR(MAX) = ', @fundata = ''' + IIF(@isfile = 1, @fundata, dbo.fn_get_xdfname( @fundata )) + ''' ';
	DECLARE @cstr AS VARCHAR(MAX) = ', @connstr = ''' + @connstr + ''' ';
	DECLARE @dist AS VARCHAR(MAX) = ', @isdistr = ' + CAST(@isdistr AS VARCHAR) + ' ';

	--TODO: need to keep a better separation between SQL and R and let outputDataSet decide the table schema if possible
	--DECLARE @result AS VARCHAR(MAX) = ' WITH RESULT SETS ((mode VARCHAR(MAX), ' + @funname + '_model VARBINARY(MAX))) ';
	DECLARE @result AS VARCHAR(MAX) = ' WITH RESULT SETS ((runmode VARCHAR(MAX), ' + @testvalues + ')) ';

	DECLARE @sql AS VARCHAR(MAX) = 
		@sp_execute_external_script + @language + @script 
		+ @input_data_1 + @input_data_1_name + @output_data_1_name 
		+ @params + @func + @args + @pargs +  @data + COALESCE(@cstr, '') + @dist
		+ @result;

	--PRINT @sql;
	EXEC(@sql);
END
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.sp_comp_rre_mdl', 'P') IS NOT NULL 
	DROP PROC dbo.sp_comp_rre_mdl;
GO
CREATE PROC dbo.sp_comp_rre_mdl
(
	@modeltable AS VARCHAR(MAX)
	, @testvalues AS VARCHAR(MAX) = NULL
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @sp_execute_external_script AS VARCHAR(MAX) = ' sp_execute_external_script ';
	DECLARE @language AS VARCHAR(MAX) = ' @language = N''R'' ';
	DECLARE @script AS VARCHAR(MAX) = ', @script = N''
		library("RUnit")

		isidentical <- TRUE
		nmodels <- nrow(inputDataSet)
		stopifnot(nmodels > 1)

		myCharToRaw <- function(myChar, split = " ")
		{
			if (nzchar(split)) {
				unserialize(as.raw(as.hexmode(strsplit(myChar, split = " ")[[1]])))
			} else {
				strlen <- nchar(myChar)
				myChar <- mapply(substr, myChar, seq(1, strlen, 2), seq(2, strlen, 2))
				unserialize(as.raw(as.hexmode(myChar)))
			}
		}

		runmode <- as.character(inputDataSet[1, "runmode"])	# varchar is read in as factor by default
		modelstr <- as.character(inputDataSet[1, "model"])	# varchar is read in as factor by default
		split <- " "	# need to match split with collapse in myRawToChar
		model <- myCharToRaw(modelstr, split = split)
		cat("* Target runmode: ", runmode, ".\n")

		testvalues <- if (exists("testvalues") && !is.na(testvalues)) strsplit(testvalues, "[[:space:]]*,[[:space:]]*")[[1]] else NULL
		#stopifnot(all(testvalues %in% names(model)))

		for (i in 2:nmodels) {
			myrunmode <- as.character(inputDataSet[i, "runmode"])
			modelstr <- as.character(inputDataSet[i, "model"])
			mymodel <- myCharToRaw(modelstr, split = split)
			cat("* Source runmode: ", myrunmode, ".\n")

			if (is.null(testvalues)) {
				cat(paste("\t- Testing for equal output.\n"))
				isidentical <- isidentical && checkEquals(model, mymodel)
				#isidentical <- isidentical && isTRUE(all.equal(model, mymodel))
			} else {
				for (tv in testvalues) {
					cat(paste("\t- Testing output component:", tv, "\n"))
					isidentical <- isidentical && checkEquals(model[tv], mymodel[tv])
					#isidentical <- isidentical && isTRUE(all.equal(model[tv], mymodel[tv]))
				}
			}
		}
		outputDataSet <- data.frame(model = gsub("#", "", modeltable), pass = isidentical)
		'' ';

	DECLARE @input_data_1 AS VARCHAR(MAX) = ', @input_data_1 = N''select * from ' + @modeltable + ''' ';
	DECLARE @input_data_1_name AS VARCHAR(MAX) = ', @input_data_1_name = N''inputDataSet'' ';
	DECLARE @output_data_1_name AS VARCHAR(MAX) = ', @output_data_1_name = N''outputDataSet'' ';

	DECLARE @params AS VARCHAR(MAX) = ', @params = N''@modeltable varchar(max)' + 
	    IIF(@testvalues IS NULL, '' , ', @testvalues varchar(max)') + ''' ';
	DECLARE @mtable AS VARCHAR(MAX) = ', @modeltable = ''' + @modeltable + ''' ';
	DECLARE @tvalues AS VARCHAR(MAX) = ', @testValues = ''' + @testvalues + ''' ';

	DECLARE @result AS VARCHAR(MAX) = ' WITH RESULT SETS ((funname VARCHAR(MAX), pass BIT)) ';

	DECLARE @sql AS VARCHAR(MAX) = 
		@sp_execute_external_script + @language + @script 
		+ @input_data_1 + @input_data_1_name + @output_data_1_name 
		+ @params + @mtable + COALESCE(@tvalues, '')
		+ @result;

	--PRINT @sql;
	EXEC(@sql);
END
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.sp_comp_rre_mdl_pred', 'P') IS NOT NULL 
	DROP PROC dbo.sp_comp_rre_mdl_pred;
GO
CREATE PROC dbo.sp_comp_rre_mdl_pred
(
	@modeltable AS VARCHAR(MAX)
	, @testvalues AS VARCHAR(MAX) = NULL
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @sp_execute_external_script AS VARCHAR(MAX) = ' sp_execute_external_script ';
	DECLARE @language AS VARCHAR(MAX) = ' @language = N''R'' ';
	DECLARE @script AS VARCHAR(MAX) = ', @script = N''
		library("RUnit")

		isidentical <- TRUE
		models <- unique(as.character(inputDataSet[["runmode"]]))
		nmodels <- length(models)
		stopifnot(nmodels > 1)

		runmode <- "local_xdf"
		model <- inputDataSet[inputDataSet[["runmode"]] == runmode, -1]
		cat("* Target runmode: ", runmode, ".\n")

		for (i in 2:nmodels) {
			myrunmode <- models[i]
			mymodel <- inputDataSet[inputDataSet[["runmode"]] == myrunmode, -1]
			cat("* Source runmode: ", myrunmode, ".\n")

			cat(paste("\t- Testing for equal output.\n"))
			isidentical <- isidentical && checkEquals(model, mymodel, check.attributes = FALSE)
			#isidentical <- isidentical && isTRUE(all.equal(model, mymodel))
		}
		outputDataSet <- data.frame(model = gsub("#", "", modeltable), pass = isidentical)
		'' ';

	DECLARE @input_data_1 AS VARCHAR(MAX) = ', @input_data_1 = N''select * from ' + @modeltable + ''' ';
	DECLARE @input_data_1_name AS VARCHAR(MAX) = ', @input_data_1_name = N''inputDataSet'' ';
	DECLARE @output_data_1_name AS VARCHAR(MAX) = ', @output_data_1_name = N''outputDataSet'' ';

	DECLARE @params AS VARCHAR(MAX) = ', @params = N''@modeltable varchar(max)' + 
	    IIF(@testvalues IS NULL, '' , ', @testvalues varchar(max)') + ''' ';
	DECLARE @mtable AS VARCHAR(MAX) = ', @modeltable = ''' + @modeltable + ''' ';
	DECLARE @tvalues AS VARCHAR(MAX) = ', @testValues = ''' + @testvalues + ''' ';

	DECLARE @result AS VARCHAR(MAX) = ' WITH RESULT SETS ((funname VARCHAR(MAX), pass BIT)) ';

	DECLARE @sql AS VARCHAR(MAX) = 
		@sp_execute_external_script + @language + @script 
		+ @input_data_1 + @input_data_1_name + @output_data_1_name 
		+ @params + @mtable + COALESCE(@tvalues, '')
		+ @result;

	--PRINT @sql;
	EXEC(@sql);
END
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.sp_exec_rre_test', 'P') IS NOT NULL 
	DROP PROC dbo.sp_exec_rre_test;
GO
CREATE PROC dbo.sp_exec_rre_test
(
	@funname AS VARCHAR(MAX)
	, @funargs AS VARCHAR(MAX)
	, @fundata AS VARCHAR(MAX)
	, @connstr AS VARCHAR(MAX)
	, @modeltable AS VARCHAR(MAX)
	, @testvalues AS VARCHAR(MAX) = NULL
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @sql NVARCHAR(MAX) = N'
		IF OBJECT_ID(''' + @modeltable + ''', ''U'') IS NOT NULL
			DROP TABLE ' + @modeltable + ';

		CREATE TABLE ' + @modeltable + '
		(
			runmode VARCHAR(MAX)
			--, model VARBINARY(MAX)
			, model VARCHAR(MAX)
		);

		-- local_xdf
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun
			@funname = @funname, @funargs = @funargs, @fundata = @fundata, @connstr = NULL, @isdistr = 0;

		-- distributed
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun
			@funname = @funname, @funargs = @funargs, @fundata = @fundata, @connstr = @connstr, @isdistr = 1;

		-- local_odbc
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun
			@funname = @funname, @funargs = @funargs, @fundata = @fundata, @connstr = @connstr, @isdistr = 0;

		-- local_df
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun
			@funname = @funname, @funargs = @funargs, @fundata = @fundata, @connstr = NULL, @isdistr = 1;

		SELECT * FROM ' + @modeltable + '

		INSERT INTO SqlServerIOQ_Tests
		EXEC dbo.sp_comp_rre_mdl
			@modeltable = ' + @modeltable + '
			, @testvalues = ''' + COALESCE(@testvalues, '') + '''
			;
		'

	--PRINT @sql;
	EXEC sp_executesql
		@stmt = @sql
		, @params = N'
			@funname AS VARCHAR(MAX)
			, @funargs AS VARCHAR(MAX)
			, @fundata AS VARCHAR(MAX)
			, @connstr AS VARCHAR(MAX)
			'
		, @funname = @funname
		, @funargs = @funargs
		, @fundata = @fundata
		, @connstr = @connstr
	;
END
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.sp_exec_rre_test_pred', 'P') IS NOT NULL 
	DROP PROC dbo.sp_exec_rre_test_pred;
GO
CREATE PROC dbo.sp_exec_rre_test_pred
(
	@funname AS VARCHAR(MAX)
	, @funargs AS VARCHAR(MAX)
	, @prdargs AS VARCHAR(MAX)
	, @fundata AS VARCHAR(MAX)
	, @connstr AS VARCHAR(MAX)
	, @modeltable AS VARCHAR(MAX)
	, @testvalues AS VARCHAR(MAX) = NULL
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @sql NVARCHAR(MAX) = N'
		IF OBJECT_ID(''' + @modeltable + ''', ''U'') IS NOT NULL
			DROP TABLE ' + @modeltable + ';

		CREATE TABLE ' + @modeltable + '
		(
			runmode VARCHAR(MAX)
			, ' + @testvalues + '
		);

		-- local_xdf
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun_pred
			@funname = @funname, @funargs = @funargs, @prdargs = @prdargs, @fundata = @fundata, @connstr = NULL, @isdistr = 0, @testvalues = @testvalues;

		-- distributed
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun_pred
			@funname = @funname, @funargs = @funargs, @prdargs = @prdargs, @fundata = @fundata, @connstr = @connstr, @isdistr = 1, @testvalues = @testvalues;

		-- local_odbc
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun_pred
			@funname = @funname, @funargs = @funargs, @prdargs = @prdargs, @fundata = @fundata, @connstr = @connstr, @isdistr = 0, @testvalues = @testvalues;

		-- local_df
		INSERT INTO ' + @modeltable + '
		EXEC dbo.sp_exec_rre_fun_pred
			@funname = @funname, @funargs = @funargs, @prdargs = @prdargs, @fundata = @fundata, @connstr = NULL, @isdistr = 1, @testvalues = @testvalues;

		SELECT * FROM ' + @modeltable + '

		INSERT INTO SqlServerIOQ_Tests
		EXEC dbo.sp_comp_rre_mdl_pred
			@modeltable = ' + @modeltable + '
			, @testvalues = ''' + COALESCE(@testvalues, '') + '''
			;
		'

	--PRINT @sql;
	EXEC sp_executesql
		@stmt = @sql
		, @params = N'
			@funname AS VARCHAR(MAX)
			, @funargs AS VARCHAR(MAX)
			, @prdargs AS VARCHAR(MAX)
			, @fundata AS VARCHAR(MAX)
			, @connstr AS VARCHAR(MAX)
			, @testvalues AS VARCHAR(MAX)
			'
		, @funname = @funname
		, @funargs = @funargs
		, @prdargs = @prdargs
		, @fundata = @fundata
		, @connstr = @connstr
		, @testvalues = @testvalues
	;
END
GO

-------------------------------------------------------------------------------
-- run a specific test with echo: sqlcmd -v funname = rxLinMod -i SqlServerIOQ_Tests.sql -e
-- run a specific test: sqlcmd -v funname = rxLinMod -i SqlServerIOQ_Tests.sql
-- run all tests: sqlcmd -v -i SqlServerIOQ_Tests.sql
-- run a specific test against a specific SQL Server: sqlcmd -v funname = rxLinMod connstr = "Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester" -i SqlServerIOQ_Tests.sql

-------------------------------------------------------------------------------
IF OBJECT_ID('SqlServerIOQ_Tests', 'U') IS NOT NULL
	DROP TABLE SqlServerIOQ_Tests;
GO

CREATE TABLE SqlServerIOQ_Tests
(
	funname VARCHAR(MAX)
	, pass BIT
);

-------------------------------------------------------------------------------
