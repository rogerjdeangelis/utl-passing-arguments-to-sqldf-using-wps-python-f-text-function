%let pgm=utl-passing-arguments-to-sqldf-using-wps-python-f-text-function;

Passing arguments to sqldf using wps python f text function;

github
https://tinyurl.com/yc7r96t3
https://github.com/rogerjdeangelis/utl-passing-arguments-to-sqldf-using-wps-python-f-text-function

Passing arguments to sqldf wps python sql functional sql;

  PROBLEM (select from iris species=versicolor and sepallength < 70 )

       arg1 =  flower   "VERSICOLOR"     (variable to subset. we want SPECIES= VERSICOLOR)
       arg2 =  sepalVar "SEPALLENGTH"    (variablename we are interested in sepallength)
       arg3 =  sepalVal "70"             ((we are interetsed in sepallengths < 70))

       select
         *
       from
         have
       where
            trim(SPECIES) = "{flower}"
        and {sepalVar}    < {sepalVal}

  THREE SOLUTIONS

       1 wps python sql functional
       2 wps python sql pass macro
       3 wps python sql string

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input
     Species $10.
     SepalLength
     SepalWidth
     PetalLength
     PetalWidth;
cards4;
VERSICOLOR 70 32 47 14
VERSICOLOR 67 31 44 14
VERSICOLOR 66 29 46 13
VERSICOLOR 67 31 47 15
VERSICOLOR 69 31 49 15
VERSICOLOR 66 30 44 14
VERSICOLOR 68 28 48 14
VERSICOLOR 67 30 50 17
VIRGINICA 67 31 56 24
VIRGINICA 69 31 51 23
VIRGINICA 68 32 59 23
VIRGINICA 77 38 67 22
VIRGINICA 67 33 57 25
VIRGINICA 76 30 66 21
VIRGINICA 67 30 52 23
VIRGINICA 79 38 64 20
VIRGINICA 67 33 57 21
VIRGINICA 77 28 67 20
VIRGINICA 72 32 60 18
VIRGINICA 77 30 61 23
VIRGINICA 72 30 58 16
VIRGINICA 71 30 59 21
VIRGINICA 77 26 69 23
VIRGINICA 69 32 57 23
VIRGINICA 74 28 61 19
VIRGINICA 73 29 63 18
VIRGINICA 67 25 58 18
VIRGINICA 69 31 54 21
VIRGINICA 72 36 61 25
VIRGINICA 68 30 55 21
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                    |                                                |                                  */
/*          INPUT                     |              PROCESS                           |          OUTPUT                  */
/*                                    |                                                |                                  */
/*  Obs     SPECIES      SEPALLENGTH  |                                                |    SPECIES      SEPALLENGTH      */
/*                                    |                                                |                                  */
/*    1    VERSICOLOR         70      |  flower   = "VERSICOLOR";                      |   VERSICOLOR         67          */
/*    2    VERSICOLOR         67      |  sepalVar = "SEPALLENGTH";                     |   VERSICOLOR         66          */
/*    3    VERSICOLOR         66      |  sepalVal = "70";                              |   VERSICOLOR         67          */
/*    4    VERSICOLOR         67      |                                                |   VERSICOLOR         69          */
/*    5    VERSICOLOR         69      |  sql = f"""select                              |   VERSICOLOR         66          */
/*    6    VERSICOLOR         66      |              *                                 |   VERSICOLOR         68          */
/*    7    VERSICOLOR         68      |            from                                |   VERSICOLOR         67          */
/*    8    VERSICOLOR         67      |              have                              |                                  */
/*    9    VIRGINICA          67      |            where                               |                                  */
/*   10    VIRGINICA          69      |                 trim(SPECIES) = "{flower}"     |                                  */
/*   11    VIRGINICA          68      |             and {sepalVar}    < {sepalVal}"""; |                                  */
/*   12    VIRGINICA          77      |                                                |                                  */
/*   13    VIRGINICA          67      |                                                |                                  */
/*   14    VIRGINICA          76      |                                                |                                  */
/*   15    VIRGINICA          67      |                                                |                                  */
/*   16    VIRGINICA          79      |                                                |                                  */
/*   17    VIRGINICA          67      |                                                |                                  */
/*   18    VIRGINICA          77      |                                                |                                  */
/*   19    VIRGINICA          72      |                                                |                                  */
/*   20    VIRGINICA          77      |                                                |                                  */
/*   21    VIRGINICA          72      |                                                |                                  */
/*   22    VIRGINICA          71      |                                                |                                  */
/*   23    VIRGINICA          77      |                                                |                                  */
/*   24    VIRGINICA          69      |                                                |                                  */
/*   25    VIRGINICA          74      |                                                |                                  */
/*   26    VIRGINICA          73      |                                                |                                  */
/*   27    VIRGINICA          67      |                                                |                                  */
/*   28    VIRGINICA          69      |                                                |                                  */
/*   29    VIRGINICA          72      |                                                |                                  */
/*   30    VIRGINICA          68      |                                                |                                  */
/*                                    |                                                |                                  */
/**************************************************************************************************************************/

/*                                    _   _                             _    __                  _   _
/ | __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |  / _|_   _ _ __   ___| |_(_) ___  _ __
| | \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | | | |_| | | | `_ \ / __| __| |/ _ \| `_ \
| |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | |  _| |_| | | | | (__| |_| | (_) | | | |
|_|   \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_| |_|  \__,_|_| |_|\___|\__|_|\___/|_| |_|
             |_|         |_|    |___/                                |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
options validvarname=any lrecl=32756;
libname sd1 "d:/sd1";
proc python;
export data=sd1.have python=have;
submit;
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
mysql = lambda q: sqldf(q, globals());

flower   = "VERSICOLOR";
sepalVar = "SEPALLENGTH";
sepalVal = "70";

sql = f"""select
            *
          from
            have
          where
               trim(SPECIES) = "{flower}"
           and {sepalVar}    < {sepalVal}""";
print(sql);
want = pdsql(sql);
print(want);
endsubmit;
import data=sd1. want python=want;
run;quit;
');

libname sd1 "d:/sd1";
proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS PYTHON Procedure                                                                                                */
/*                                                                                                                        */
/*       SPECIES  SEPALLENGTH  SEPALWIDTH  PETALLENGTH  PETALWIDTH                                                        */
/* 0  VERSICOLOR         67.0        31.0         44.0        14.0                                                        */
/* 1  VERSICOLOR         66.0        29.0         46.0        13.0                                                        */
/* 2  VERSICOLOR         67.0        31.0         47.0        15.0                                                        */
/* 3  VERSICOLOR         69.0        31.0         49.0        15.0                                                        */
/* 4  VERSICOLOR         66.0        30.0         44.0        14.0                                                        */
/* 5  VERSICOLOR         68.0        28.0         48.0        14.0                                                        */
/* 6  VERSICOLOR         67.0        30.0         50.0        17.0                                                        */
/*                                                                                                                        */
/*                                                                                                                        */
/* WPS BASE                                                                                                               */
/*                                                                                                                        */
/*    SPECIES      SEPALLENGTH    SEPALWIDTH    PETALLENGTH    PETALWIDTH                                                 */
/*                                                                                                                        */
/*   VERSICOLOR         67            31             44            14                                                     */
/*   VERSICOLOR         66            29             46            13                                                     */
/*   VERSICOLOR         67            31             47            15                                                     */
/*   VERSICOLOR         69            31             49            15                                                     */
/*   VERSICOLOR         66            30             44            14                                                     */
/*   VERSICOLOR         68            28             48            14                                                     */
/*   VERSICOLOR         67            30             50            17                                                     */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                     _   _                             _
|___ \  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |  _ __   __ _ ___ ___   _ __ ___   __ _  ___ _ __ ___
  __) | \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | | | `_ \ / _` / __/ __| | `_ ` _ \ / _` |/ __| `__/ _ \
 / __/   \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | | |_) | (_| \__ \__ \ | | | | | | (_| | (__| | | (_) |
|_____|   \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_| | .__/ \__,_|___/___/ |_| |_| |_|\__,_|\___|_|  \___/
                 |_|         |_|    |___/                                |_|   |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x(resolve('
options validvarname=any lrecl=32756;
libname sd1 "d:/sd1";
%let flower   = VERSICOLOR;
%let sepalVar = SEPALLENGTH;
%let sepalVal = 70;
proc python;
export data=sd1.have python=have;
submit;
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
mysql = lambda q: sqldf(q, globals());
sql = f"""select * from have where trim(SPECIES) = "&flower" and &sepalVar < &sepalVal""";
print(sql);
want = pdsql(sql);
print(want);
endsubmit;
import data=sd1. want python=want;
run;quit;
'));

libname sd1 "d:/sd1";
proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS PYTHON Procedure                                                                                               */
/*                                                                                                                        */
/*       SPECIES  SEPALLENGTH  SEPALWIDTH  PETALLENGTH  PETALWIDTH                                                        */
/* 0  VERSICOLOR         67.0        31.0         44.0        14.0                                                        */
/* 1  VERSICOLOR         66.0        29.0         46.0        13.0                                                        */
/* 2  VERSICOLOR         67.0        31.0         47.0        15.0                                                        */
/* 3  VERSICOLOR         69.0        31.0         49.0        15.0                                                        */
/* 4  VERSICOLOR         66.0        30.0         44.0        14.0                                                        */
/* 5  VERSICOLOR         68.0        28.0         48.0        14.0                                                        */
/* 6  VERSICOLOR         67.0        30.0         50.0        17.0                                                        */
/*                                                                                                                        */
/*                                                                                                                        */
/* WPS BASE                                                                                                               */
/*                                                                                                                        */
/*    SPECIES      SEPALLENGTH    SEPALWIDTH    PETALLENGTH    PETALWIDTH                                                 */
/*                                                                                                                        */
/*   VERSICOLOR         67            31             44            14                                                     */
/*   VERSICOLOR         66            29             46            13                                                     */
/*   VERSICOLOR         67            31             47            15                                                     */
/*   VERSICOLOR         69            31             49            15                                                     */
/*   VERSICOLOR         66            30             44            14                                                     */
/*   VERSICOLOR         68            28             48            14                                                     */
/*   VERSICOLOR         67            30             50            17                                                     */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                    _   _                             _       _        _
|___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |  ___| |_ _ __(_)_ __   __ _
  |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | | / __| __| `__| | `_ \ / _` |
 ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | \__ \ |_| |  | | | | | (_| |
|____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_| |___/\__|_|  |_|_| |_|\__, |
                 |_|         |_|    |___/                                |_|                         |___/
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
options validvarname=any lrecl=32756;
libname sd1 "d:/sd1";
proc python;
export data=sd1.have python=have;
submit;
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
mysql = lambda q: sqldf(q, globals());
flower   = "VERSICOLOR";
sepalVar = "SEPALLENGTH";
sepalVal = "70";
sql = "select * from have where trim(SPECIES) = "
          + "\"" + flower + "\""
          + " and " + sepalVar + "<" + sepalVal;
print(sql);
want = pdsql(sql);
print(want);
endsubmit;
import data=sd1. want python=want;
run;quit;
');

libname sd1 "d:/sd1";
proc print data=sd1.want;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS PYTHON Procedure                                                                                                */
/*                                                                                                                        */
/*       SPECIES  SEPALLENGTH  SEPALWIDTH  PETALLENGTH  PETALWIDTH                                                        */
/* 0  VERSICOLOR         67.0        31.0         44.0        14.0                                                        */
/* 1  VERSICOLOR         66.0        29.0         46.0        13.0                                                        */
/* 2  VERSICOLOR         67.0        31.0         47.0        15.0                                                        */
/* 3  VERSICOLOR         69.0        31.0         49.0        15.0                                                        */
/* 4  VERSICOLOR         66.0        30.0         44.0        14.0                                                        */
/* 5  VERSICOLOR         68.0        28.0         48.0        14.0                                                        */
/* 6  VERSICOLOR         67.0        30.0         50.0        17.0                                                        */
/*                                                                                                                        */
/*                                                                                                                        */
/* WPS BASE                                                                                                               */
/*                                                                                                                        */
/*    SPECIES      SEPALLENGTH    SEPALWIDTH    PETALLENGTH    PETALWIDTH                                                 */
/*                                                                                                                        */
/*   VERSICOLOR         67            31             44            14                                                     */
/*   VERSICOLOR         66            29             46            13                                                     */
/*   VERSICOLOR         67            31             47            15                                                     */
/*   VERSICOLOR         69            31             49            15                                                     */
/*   VERSICOLOR         66            30             44            14                                                     */
/*   VERSICOLOR         68            28             48            14                                                     */
/*   VERSICOLOR         67            30             50            17                                                     */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
