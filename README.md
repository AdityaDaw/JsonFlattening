# JsonFlattening
This is a spark project for flattening Json and convert it to dataframe. This is a one to one flattening logic

## Lets Assume this is the json string which we want to flatten
"L0_1": 1,"L0_S1": {"L1_1": "a","L1_2": 2},"L0_2": "b","L0_3": [1,2,3],"L1_AR1": [5,6,7],"L0_4": {"L1_5": "d","l1_6": ["Rama","Krishna"]}}

## Before flattening the the Data Frame will be treated in Spark as
+----+----+---------+--------------------+------+---------+
|L0_1|L0_2|     L0_3|                L0_4| L0_S1|   L1_AR1|
+----+----+---------+--------------------+------+---------+
|   1|   b|[1, 2, 3]|[d, [Rama, Krishna]]|[a, 2]|[5, 6, 7]|
+----+----+---------+--------------------+------+---------+
root
 |-- L0_1: long (nullable = true)
 |-- L0_2: string (nullable = true)
 |-- L0_3: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- L0_4: struct (nullable = true)
 |    |-- L1_5: string (nullable = true)
 |    |-- l1_6: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- L0_S1: struct (nullable = true)
 |    |-- L1_1: string (nullable = true)
 |    |-- L1_2: long (nullable = true)
 |-- L1_AR1: array (nullable = true)
 |    |-- element: long (containsNull = true)
 
 # After this one to one flattening logic the the data frame will be converted to 
 +----+----+----------+-----------+-----------+----+------+----------+
|L0_1|L0_2|L0_4__L1_5|L0_S1__L1_1|L0_S1__L1_2|L0_3|L1_AR1|L0_4__l1_6|
+----+----+----------+-----------+-----------+----+------+----------+
|   1|   b|         d|          a|          2|   1|     5|      Rama|
|   1|   b|         d|          a|          2|   2|     6|   Krishna|
|   1|   b|         d|          a|          2|   3|     7|      null|
+----+----+----------+-----------+-----------+----+------+----------+

root
 |-- L0_1: long (nullable = true)
 |-- L0_2: string (nullable = true)
 |-- L0_4__L1_5: string (nullable = true)
 |-- L0_S1__L1_1: string (nullable = true)
 |-- L0_S1__L1_2: long (nullable = true)
 |-- L0_3: long (nullable = true)
 |-- L1_AR1: long (nullable = true)
 |-- L0_4__l1_6: string (nullable = true)
