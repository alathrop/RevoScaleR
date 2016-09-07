USE RevoTestDB;

-------------------------------------------------------------------------------
---- To run a specific test in SSMS, set Query -> SQLCMD Mode and uncomment the following SQLCMD commands

---- SQLCMD commands
--:setvar funname rxDTree
--:setvar connstr "Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Trusted_Connection=True;"
--:setvar connstr "Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd"
--:setvar connstr "Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd"
--!!echo %cd%

---- T-SQL statements
SELECT @@SERVERNAME AS 'Server Name', @@VERSION AS 'Server Version', @@SERVICENAME AS 'Service Name'

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxSummary' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxSummary';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ~ age + car.age + type + cost + number';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'nobs.valid, nobs.missing, sDataFrame, categorical, formula, categorical.type';	--, c("params", "call")

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxCrossTabs' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxCrossTabs';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = cost ~ age : car.age : type';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'sums, counts, chisquare, ndepvars, depvars, validObs, missingObs, formula';	--, c("params", "call")

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxCube' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxCube';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = number ~ age : car.age : type';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'age, car.age, type, number, Counts';	--NOTE: other components are in attributes!

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxLinMod' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxLinMod';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = cost ~ age + type + number';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';
	
	--PRINT '$(SQLCMDUSER)';
	--PRINT '$(SQLCMDPASSWORD)';	--cannot retrieve the the password specified on the command line
	--PRINT '$(SQLCMDWORKSTATION)';
	--PRINT '$(SQLCMDSERVER)';
	--PRINT '$(SQLCMDDBNAME)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'coefficients, residual.squares, condition.number, rank, coef.std.error, coef.t.value, df';	--, params

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;

	DECLARE @myprdargs VARCHAR(MAX) = 'extraVarsToWrite = "RowNum", overwrite = TRUE';
	SET @modeltable = @modeltable + '_rxPredict';
	SET @testvalues = 'Pred FLOAT, RowNum INT';
	EXEC dbo.sp_exec_rre_test_pred
		@funname = @myfunname
		, @funargs = @myfunargs
		, @prdargs = @myprdargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxCovCor' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxCovCor';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ~ age + car.age + type + cost + number';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'CovCor, DroppedVars, DroppedVarIndexes, StdDevs, Means, valid.obs, missing.obs, SumOfWeights, formula';	--, c("params", "call")

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxCov' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxCov';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ~ age + car.age + type + cost + number';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = '';	--NOTE: use '' or NULL to test all components

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxCor' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxCor';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ~ age + car.age + type + cost + number';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = NULL;	--NOTE: use '' or NULL to test all components

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxSSCP' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxSSCP';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ~ age + car.age + type + cost + number';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = NULL;	--NOTE: use '' or NULL to test all components

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		--, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxLogit' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxLogit';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ageLogical ~ car.age + type + cost + number, transforms = expression(list(ageLogical = (age == "30-34")))';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = '
		coefficients, residual.squares, condition.number, rank, aliased, coef.std.error, coef.t.value, coef.p.value, total.squares, y.var, 
		sigma, residual.variance, f.pvalue, df, y.names, deviance, aic, dispersion, iterations, formula, fstatistics, nValidObs, coefLabelStyle
		';	--, c("params", "call", "nMissingObs")

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;

	DECLARE @myprdargs VARCHAR(MAX) = 'extraVarsToWrite = "RowNum", overwrite = TRUE';
	SET @modeltable = @modeltable + '_rxPredict';
	SET @testvalues = 'Pred FLOAT, RowNum INT';
	EXEC dbo.sp_exec_rre_test_pred
		@funname = @myfunname
		, @funargs = @myfunargs
		, @prdargs = @myprdargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxGlm' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxGlm';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ageLogical ~ car.age + type + cost + number, family = binomial(), transforms = expression(list(ageLogical = (age == "30-34")))';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = '
		coefficients, condition.number, rank, aliased, coef.std.error, coef.t.value, coef.p.value, f.pvalue, 
		df, deviance, aic, dispersion, iterations, family, formula, fstatistics, nValidObs, coefLabelStyle
		';	--, c("params", "call", "nMissingObs")

	--NOTE: An INSERT EXEC statement cannot be nested.
	--INSERT INTO SqlServerIOQ_Tests
	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;

	DECLARE @myprdargs VARCHAR(MAX) = 'extraVarsToWrite = "RowNum", overwrite = TRUE';
	SET @modeltable = @modeltable + '_rxPredict';
	SET @testvalues = 'Pred FLOAT, RowNum INT';
	EXEC dbo.sp_exec_rre_test_pred
		@funname = @myfunname
		, @funargs = @myfunargs
		, @prdargs = @myprdargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxKmeans' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxKmeans';
	--DECLARE @myfunargs VARCHAR(MAX) = 'formula = ~ cost + number, numClusters = 3, seed = 42';	--why failed?
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = ~ cost + number, centers = matrix(c(209, 33, 753, 25, 263, 221), byrow = TRUE, ncol = 2)'
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd';
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = '
		centers, size, withinss, valid.obs, missing.obs, numIterations, tot.withinss, totss, betweenss, formula
		';	--, c("params", "call", "cluster")

	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxDTree' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxDTree';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = age ~ car.age + type + cost + number, minSplit = 5, maxDepth = 2, xVal = 0, maxNumBins = 200';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd'
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'frame, cptable, splits, csplit, method, parms, control, ordered';	--, where

	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;

	DECLARE @myprdargs VARCHAR(MAX) = 'extraVarsToWrite = "RowNum", overwrite = TRUE';
	SET @modeltable = @modeltable + '_rxPredict';
	SET @testvalues = '[17-20_prob] FLOAT, [21-24_prob] FLOAT, [25-29_prob] FLOAT, [30-34_prob] FLOAT, [35-39_prob] FLOAT, [40-49_prob] FLOAT, [50-59_prob] FLOAT, [60+_prob] FLOAT, RowNum INT';
	EXEC dbo.sp_exec_rre_test_pred
		@funname = @myfunname
		, @funargs = @myfunargs
		, @prdargs = @myprdargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxDForest' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxDForest';
	DECLARE @myfunargs VARCHAR(MAX) = 'formula = age ~ car.age + type + cost + number, minSplit = 5, maxDepth = 2, nTree = 2, maxNumBins = 200, seed = 0';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd'
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'ntree, mtry, type, cutoff, formula';	--, "forest", "oob.err", "confusion", "params", "call"

	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
IF '$(funname)' = 'rxBTrees' OR '$(funname)' LIKE '$%'
BEGIN
	DECLARE @myfunname VARCHAR(MAX) = 'rxBTrees';
	DECLARE @myfunargs VARCHAR(MAX) = ' \
		formula = ageLogical ~ car.age + type + cost + number, transforms = expression(list(ageLogical = (age == "30-34"))), \
		lossFunction = "bernoulli", learningRate = 0.1, minSplit = 5, maxDepth = 1, nTree = 2, maxNumBins = 200, seed = 0';
	DECLARE @myfundata VARCHAR(MAX) = 'claims.xdf';

	DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=.;Database=RevoTestDB;Uid=RevoTester;Pwd=RevoTester';
	--DECLARE @myconnstr VARCHAR(MAX) = 'Driver=SQL Server;Server=revosqlsvr1601.corp.microsoft.com;Port=1433;Database=RevoTestDB;Uid=RevoTester;Pwd=T3sterPwd'
	IF '$(connstr)' NOT LIKE '$%' SET @myconnstr = '$(connstr)';

	DECLARE @modeltable VARCHAR(MAX) = '#' + @myfunname;
	DECLARE @testvalues VARCHAR(MAX) = 'ntree, mtry, type, cutoff, formula';	--, "forest", "oob.err", "confusion", "params", "call"

	EXEC dbo.sp_exec_rre_test
		@funname = @myfunname
		, @funargs = @myfunargs
		, @fundata = @myfundata
		, @connstr = @myconnstr
		, @modeltable = @modeltable
		, @testvalues = @testvalues
		;
END
GO

-------------------------------------------------------------------------------
--TODO: add more tests here

-------------------------------------------------------------------------------
SELECT * FROM SqlServerIOQ_Tests;

-------------------------------------------------------------------------------
