GO

CREATE TABLE [dbo].[tal_dly_mail](
	[Entry_ID] [int] IDENTITY(1,1) NOT NULL,
	[Code] [int] NULL,
	[Name] [varchar](50) NULL,
	[Header] [varchar](200) NULL,
	[Start_time] [time](7) NULL,
	[End_time] [time](7) NULL,
	[Day] [varchar](50) NULL,
	[Status] [char](1) NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

