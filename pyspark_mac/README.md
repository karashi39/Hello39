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
