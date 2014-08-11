
def createTable(tableName):

   if tableName == "RXNCONSO.RRF":
      query = """CREATE TABLE rxnconso
      (
         rxcui             varchar(8) NOT NULL,
         lat               varchar (3) NOT NULL,
         rxaui             varchar(8) NOT NULL,
         saui              varchar(50),
         scui              varchar(50),
         sdui              varchar(50),
         sab               varchar (20) NOT NULL,
         tty               varchar (20) NOT NULL,
         code              varchar (50) NOT NULL,
         str               varchar (3000) NOT NULL,
         suppress          varchar (1),
         cvf               varchar(50)
      );"""

   elif tableName == "RXNREL.RRF":
      query = """CREATE TABLE rxnrel
      (
         rxcui1     varchar(8),
         rxaui1     varchar(8),
         stype1     varchar(50),
         rel        varchar(4),
         rxcui2     varchar(8),
         rxaui2     varchar(8),
         stype2     varchar(50),
         rela       varchar(100),
         rui        varchar(10),
         sab        varchar(20) NOT NULL,
         dir        varchar(1),
         cvf        varchar(50)
      );"""

   elif tableName == "RXNSAB.RRF":
      query = """ CREATE TABLE rxnsab
      (
         vcui           varchar (8),
         rcui           varchar (8),
         vsab           varchar (40),
         rsab           varchar (20) NOT NULL,
         son            varchar (3000),
         sf             varchar (20),
         sver           varchar (20),
         vstart         varchar(10),
         vend           varchar(10),
         imeta          varchar (10),
         slc            varchar (1000),
         scc            varchar(1000),
         srl            integer,
         cxty           varchar(50),
         ttyl           varchar (300),
         atnl           varchar(1000),
         lat            varchar (3),
         cenc           varchar (20),
         curver         varchar (1),
         sabin          varchar (1),
         ssn            varchar (3000),
         scit           varchar (4000)
      );"""

   elif tableName == "RXNSAT.RRF":
      query = """CREATE TABLE rxnsat
      (
         rxcui            varchar(8),
         rxaui            varchar(8),
         stype            varchar (50),
         code             varchar(50),
         atui             varchar(11),
         atn              varchar (1000) NOT NULL,
         sab              varchar (20) NOT NULL,
         atv              varchar (4000),
         suppress         varchar (1),
         cvf              varchar(50)
      );"""

   elif tableName == "RXNSTY.RRF":
      query = """CREATE TABLE rxnsty
      (
         rxcui          varchar(8) NOT NULL,
         tui            varchar (4),
         stn            varchar (100),
         sty            varchar (50),
         cvf            varchar(50)
      );"""
   
   elif tableName == "RXNDOC.RRF":
      query = """CREATE TABLE rxndoc (
          key         varchar(50),
          value       varchar (1000),
          type        varchar (50) NOT NULL,
          expl        varchar (1000)
      );"""

   elif tableName == "RXNCUI.RRF":
      query = """CREATE TABLE rxncui (
       cui1         VARCHAR(8),
       vsab_start   VARCHAR(40),
       vsas_end     VARCHAR(40),
       cardinality  VARCHAR(8),
       cui2         VARCHAR(8) 
      );"""
   else:
      query = None

   return query



