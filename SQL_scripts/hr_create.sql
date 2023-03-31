GO

CREATE TABLE [dbo].[tal_hr_mail](
	[Hr_id] [int] IDENTITY(1,1) NOT NULL,
	[Group_type] [varchar](20) NULL,
	[Email_IDs] [varchar](max) NULL,
	[File_name] [varchar](max) NULL,
	[Body] [varchar](200) NULL,
	[Subject] [varchar](200) NULL,
	[Start_time] [time](7) NULL,
	[End_time] [time](7) NULL,
	[Day] [varchar](50) NULL,
	[Status] [char](1) NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO