
def createTable(tableName):

   if tableName == "RXNCONSO.RRF":
      query = """CREATE TABLE rxnconso
      (
         rxcui             varchar(8) NOT NULL,
         lat               varchar (3) NOT NULL,
         rxaui             varchar(8) NOT NULL,
         sab               varchar (20) NOT NULL,
         tty               varchar (20) NOT NULL,
         code              varchar (50) NOT NULL,
         str               varchar (3000) NOT NULL,
         suppress          varchar (1)
      );"""

   elif tableName == "RXNREL.RRF":
      query = """CREATE TABLE rxnrel
      (
         rxaui1    varchar(8),
         stype1    varchar(50),
         rel       varchar(4) ,
         rxaui2    varchar(8),
         stype2    varchar(50),
         sab       varchar(20) NOT NULL
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
         imeta          varchar (10),
         slc            varchar (1000),
         srl            integer,
         ttyl           varchar (300),
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
         rxaui            varchar(8),
         stype            varchar (50),
         atn              varchar (1000) NOT NULL,
         sab              varchar (20) NOT NULL,
         atv              varchar (4000),
         suppress         varchar (1)
      );"""

   elif tableName == "RXNSTY.RRF":
      query = """CREATE TABLE rxnsty
      (
         rxcui          varchar(8) NOT NULL,
         tui            varchar (4),
         stn            varchar (100),
         sty            varchar (50)
      );"""
   
   elif tableName == "RXNDOC.RRF":
      query = """CREATE TABLE rxndoc (
          name        varchar (1000),
          type        varchar (50) NOT NULL,
          expl        varchar (1000)
      );"""

   elif tableName == "RXNCUI.RRF":
      query = """CREATE TABLE rxncui (
       cui1 VARCHAR(8),
       vsab_start VARCHAR(40),
       vsas_end   VARCHAR(40),
       cardinality VARCHAR(8),
       cui2       VARCHAR(8) 
      );"""
   else:
      query = None

   return query



