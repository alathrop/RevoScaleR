USE RevoTestDB;

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.fn_get_xdfname', 'FN') IS NOT NULL
	DROP FUNCTION dbo.fn_get_xdfname;
GO

IF OBJECT_ID('dbo.fn_get_tblname', 'FN') IS NOT NULL
	DROP FUNCTION dbo.fn_get_tblname;
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.sp_exec_rre_fun', 'P') IS NOT NULL
	DROP PROC dbo.sp_exec_rre_fun;
GO

IF OBJECT_ID('dbo.sp_comp_rre_mdl', 'P') IS NOT NULL
	DROP PROC dbo.sp_comp_rre_mdl;
GO

IF OBJECT_ID('dbo.sp_exec_rre_test', 'P') IS NOT NULL
	DROP PROC dbo.sp_exec_rre_test;
GO

-------------------------------------------------------------------------------
IF OBJECT_ID('SqlServerIOQ_Tests', 'U') IS NOT NULL
	DROP TABLE SqlServerIOQ_Tests;
GO

-------------------------------------------------------------------------------
