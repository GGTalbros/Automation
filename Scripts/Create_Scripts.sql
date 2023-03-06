GO
CREATE TABLE [dbo].[supp_adt_trl](
	[audit_id] [int] IDENTITY(1,1) NOT NULL,
	[cardcode] [varchar](15) NULL,
	[module] [varchar](20) NULL,
	[path] [varchar](300) NULL,
	[data] [varbinary](max) NULL,
	[datecreated] [datetime] NULL,
	[status] [varchar](15) NULL,
	[error_msg] [varchar](200) NULL,
	[text_data] [varchar](400) NULL,
 CONSTRAINT [pk_audit_id] PRIMARY KEY CLUSTERED 
(
	[audit_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO



CREATE TABLE [dbo].[supp_int_data](
	[GR_dt] [date] NULL,
	[GR_no] [varchar](20) NULL,
	[DC_no] [varchar](20) NULL,
	[Rej_qty] [varchar](20) NULL,
	[GR_qty] [varchar](20) NULL,
	[WSN_ASN] [varchar](30) NULL,
	[Datecreated] [datetime] NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


/****** Object:  Table [dbo].[supp_prc_txns]    Script Date: 20-02-2023 1.15.03 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[supp_prc_txns](
	[txn_id] [int] IDENTITY(1,1) NOT NULL,
	[cardcode] [varchar](100) NOT NULL,
	[party_partno] [varchar](100) NOT NULL,
	[old_inv_prc] [numeric](18, 4) NULL,
	[new_po_prc] [numeric](18, 4) NULL,
	[supp_date] [datetime] NULL,
	[supp_date_end] [datetime] NULL,
	[datecreated] [datetime] NULL,
	[status] [char](1) NULL,
 CONSTRAINT [pk_txn_id] PRIMARY KEY CLUSTERED 
(
	[txn_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_supp_prc_txns] UNIQUE NONCLUSTERED 
(
	[cardcode] ASC,
	[party_partno] ASC,
	[supp_date] ASC,
	[status] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO



CREATE TABLE [dbo].[supp_txns](
	[txn_id] [int] IDENTITY(1,1) NOT NULL,
	[cardcode] [varchar](100) NOT NULL,
	[party_partno] [varchar](100) NOT NULL,
	[supp_date] [datetime] NOT NULL,
	[dr_no] [int] NOT NULL,
	[dr_date] [date] NULL,
	[org_inv_no] [int] NOT NULL,
	[org_inv_date] [datetime] NOT NULL,
	[po_no] [varchar](200) NOT NULL,
	[old_prc] [numeric](18, 4) NULL,
	[new_prc] [numeric](18, 4) NULL,
	[status] [char](1) NULL,
	[datecreated] [datetime] NULL,
	[dateupdated] [datetime] NULL,
 CONSTRAINT [pk_supp_txn_id] PRIMARY KEY CLUSTERED 
(
	[txn_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_supp_txns] UNIQUE NONCLUSTERED 
(
	[cardcode] ASC,
	[party_partno] ASC,
	[supp_date] ASC,
	[org_inv_no] ASC,
	[status] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

