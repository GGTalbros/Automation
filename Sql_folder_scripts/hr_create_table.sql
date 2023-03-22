GO

CREATE TABLE [dbo].[tal_hr_mail](
	[Hr_id] [int] IDENTITY(1,1) NOT NULL,
	[Group_type] [varchar](20) NULL,
	[Email_IDs] [varchar](50) NULL,
	[Start_time] [time](7) NULL,
	[End_time] [time](7) NULL,
	[Day] [varchar](50) NULL,
	[Status] [char](1) NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO
