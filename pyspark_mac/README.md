# pyspark_mac

## Requirement

- JDK

## Usage

```
$ brew install apache-spark
$ . env.sh
$ python hello.py
```

## Result

```
+-----+
|hello|
+-----+
|world|
+-----+
```

## Other Sample

### cross_join

example for cross join and shows how to improve performance of it.

#### Usage and Result

```
$ python cross_join.py data/beatles.csv data/queen.csv
$ python cross_join_once_convert.py data/beatles.csv data/queen.csv
```

```
+-------+---------+
|   band|     part|
+-------+---------+
|Beatles|    Sitar|
|Beatles|     Base|
|Beatles|     Moog|
|Beatles|Mellotron|
|Beatles|    Vocal|
|Beatles|    Drums|
|Beatles|   Guitar|
|Beatles|    Piano|
|  Queen|    Sitar|
|  Queen|     Base|
|  Queen|     Moog|
|  Queen|Mellotron|
|  Queen|    Vocal|
|  Queen|    Drums|
|  Queen|   Guitar|
|  Queen|    Piano|
+-------+---------+
```

#### cross_join.py

1. create population data from all input.
1. create master table of band with distinct.
1. create master table of part with distinct.
1. cross join the mastar tables.

the script will end in 60s.

#### cross_join_once_convert.py

almost same process but added convert processdata frame into list type once.

the script will end in 12s.
this is one-fifth of non-converted cross join.

#### appendix

use time_disp_py.sh to display exec time of python script as below.

```
$ sh ../benri/time_disp_py.sh cross_join.py data/beatles.csv
```

### pivot.py

sample code for make pivot table

```
$ python pivot.py data/beatles.csv data/queen.csv
```

#### original table

```
+-------+---------+-------+
|   band|     part| member|
+-------+---------+-------+
|Beatles|    Vocal|   John|
|Beatles|   Guitar|   John|
|Beatles|    Piano|   John|
|Beatles|    Vocal|   Paul|
|Beatles|     Base|   Paul|
|Beatles|     Moog|   Paul|
|Beatles|Mellotron|   Paul|
|Beatles|    Vocal| George|
|Beatles|   Guitar| George|
|Beatles|    Sitar| George|
|Beatles|     Moog| George|
|Beatles|    Vocal|  Ringo|
|Beatles|    Drums|  Ringo|
|  Queen|    Vocal|Freddie|
|  Queen|    Piano|Freddie|
|  Queen|   Guitar|Freddie|
|  Queen|    Vocal|  Brian|
|  Queen|   Guitar|  Brian|
|  Queen|    Piano|  Brian|
|  Queen|     Base|   John|
+-------+---------+-------+
```

#### pivot code

```
df_pivot = df_all.groupBy(['band']).pivot('part').agg(first('member'))
```

#### pivoted table

```
+-------+----+-----+-------+---------+----+-------+------+-------+
|   band|Base|Drums| Guitar|Mellotron|Moog|  Piano| Sitar|  Vocal|
+-------+----+-----+-------+---------+----+-------+------+-------+
|Beatles|Paul|Ringo|   John|     Paul|Paul|   John|George|   John|
|  Queen|John|Roger|Freddie|     null|null|Freddie|  null|Freddie|
+-------+----+-----+-------+---------+----+-------+------+-------+
```

### bad.py

sample code describes that syntax checker don't work on spark.
it is based on pivot.py

```
$ python bad.py data/beatles.csv
```

after execute python script, spark will process and exit with Attribute Error.  
like below.

![exec bad.py](https://github.com/karashi39/Hello39/blob/image/images/bad.py.gif)
