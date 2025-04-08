SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE DATABASE framework;
GO
USE framework;
GO

-- Create Error Message Reference table with IDENTITY
CREATE TABLE [dbo].[FW_Error_Message_Reference]
(
    [Error_Id]      [int] IDENTITY (1,1) NOT NULL,
    [Error_Message] [varchar](255)       NOT NULL,
    PRIMARY KEY CLUSTERED ([Error_Id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Create File Category table (Schema_Text column removed)
CREATE TABLE [dbo].[FW_File_Category]
(
    [File_Category_Id]   [int] IDENTITY (1,1) NOT NULL,
    [File_Category_Name] [varchar](255)       NOT NULL,
    PRIMARY KEY CLUSTERED ([File_Category_Id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Create File Name table
CREATE TABLE [dbo].[FW_File_Name]
(
    [File_Id]          [int] IDENTITY (1,1) NOT NULL,
    [File_Category_Id] [int]                NOT NULL,
    [File_Name]        [varchar](255)       NOT NULL,
    PRIMARY KEY CLUSTERED ([File_Id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Create Pipeline Observability table
CREATE TABLE [dbo].[FW_Pipeline_Observability]
(
    [Processing_File_Id]         [int] IDENTITY (1,1) NOT NULL,
    [File_Id]                    [int]                NOT NULL,
    [Time_Of_Arrival]            [datetime]           NOT NULL,
    [Process_StartTime]          [datetime]           NOT NULL,
    [Process_End_Time]           [datetime]           NOT NULL,
    [Input_File_Size]            [varchar](255)       NOT NULL,
    [Initial_Count_Of_Records]   [int]                NOT NULL,
    [Count_Of_Processed_Records] [int]                NOT NULL,
    [Count_Of_Error_Records]     [int]                NOT NULL,
    [Count_of_Distinct_Errors]   [int]                NOT NULL,
    PRIMARY KEY CLUSTERED ([Processing_File_Id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Create File Record Error table
CREATE TABLE [dbo].[FW_File_Record_Error]
(
    [File_Record_ID]     [int] IDENTITY (1,1) NOT NULL,
    [Record_ID]          [int]                NOT NULL,
    [Processing_File_Id] [int]                NOT NULL,
    [Record_Text]        [varchar](1500)      NOT NULL,
    PRIMARY KEY CLUSTERED ([File_Record_ID] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Create Column Error table
CREATE TABLE [dbo].[FW_Column_Error]
(
    [File_Record_ID] [int]          NOT NULL,
    [Column_Name]    [varchar](255) NOT NULL,
    [Error_Id]       [int]          NOT NULL,
    [Error_Code]     [int]          NOT NULL,
    CONSTRAINT [PK_FW_Column_Error] PRIMARY KEY CLUSTERED ([File_Record_ID], [Column_Name], [Error_Id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Add foreign key constraints
ALTER TABLE [dbo].[FW_File_Name]
    WITH CHECK ADD CONSTRAINT
        [FK_FW_File_Name_FW_File_Category] FOREIGN KEY ([File_Category_Id])
            REFERENCES [dbo].[FW_File_Category] ([File_Category_Id])
GO

ALTER TABLE [dbo].[FW_Pipeline_Observability]
    WITH CHECK ADD CONSTRAINT
        [FK_FW_Pipeline_Observability_FW_File_Name] FOREIGN KEY ([File_Id])
            REFERENCES [dbo].[FW_File_Name] ([File_Id])
GO

-- ALTER TABLE [dbo].[FW_File_Record_Error]
--     WITH CHECK ADD CONSTRAINT
--         [FK_FW_File_Record_Error_FW_Pipeline_Observability] FOREIGN KEY ([Processing_File_Id])
--             REFERENCES [dbo].[FW_Pipeline_Observability] ([Processing_File_Id])
-- GO

ALTER TABLE [dbo].[FW_Column_Error]
    WITH CHECK ADD CONSTRAINT
        [FK_FW_Column_Error_FW_Error_Message_Reference] FOREIGN KEY ([Error_Id])
            REFERENCES [dbo].[FW_Error_Message_Reference] ([Error_Id])
GO

ALTER TABLE [dbo].[FW_Column_Error]
    WITH CHECK ADD CONSTRAINT
        [FK_FW_Column_Error_FW_File_Record_Error] FOREIGN KEY ([File_Record_ID])
            REFERENCES [dbo].[FW_File_Record_Error] ([File_Record_ID])
GO

-- Add CHECK constraints
ALTER TABLE [dbo].[FW_File_Name]
    CHECK CONSTRAINT
        [FK_FW_File_Name_FW_File_Category]
GO

ALTER TABLE [dbo].[FW_Pipeline_Observability]
    CHECK CONSTRAINT
        [FK_FW_Pipeline_Observability_FW_File_Name]
GO

-- ALTER TABLE [dbo].[FW_File_Record_Error]
--     CHECK CONSTRAINT
--         [FK_FW_File_Record_Error_FW_Pipeline_Observability]
-- GO

ALTER TABLE [dbo].[FW_Column_Error]
    CHECK CONSTRAINT
        [FK_FW_Column_Error_FW_Error_Message_Reference]
GO

ALTER TABLE [dbo].[FW_Column_Error]
    CHECK CONSTRAINT
        [FK_FW_Column_Error_FW_File_Record_Error]
GO