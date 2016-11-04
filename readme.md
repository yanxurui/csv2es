# import csv into elasticsearch

## features
1. load data from csv file exported from mysql by sequel pro into es by bulk requests
2. use a specific field as customized _id
3. ignore lines with format error
4. support big file
5. load from a offset

## usage
```
python csv2es.py --es-host 192.168.197.128:9200 --delimiter , --bulk-size 1000 --index news --type article1000 --id-field id --offset 23000 article.csv
```

## requirements
* python2
* click
* elasticsearch-py

## limit
#### only support csv file with strict format
* use , as delimiter
* string is always wrapped by “
* newline character in a string must be expressed as \n because every line is parsed as a document
* null

#### line example
```
1, 2.2, "a", "a,b\nc", "他说:\"中国...", true, null, "2016-11-03 11:37:27"
```

#### why
Python's built-in cvs module cannot detect the type of a field and it treats every field as a str. This is not what we expect as null will be “null” in the request body when we index and then ES will treat it as string “null” instead of null.
This code use json module to load every line as a list so the format must satisfy what json document requires.


## todo
- [ ] multithreads support
Both ThreadPoolExecutor from concurrent.futures and Parallel from joblib require tasks to be prepared before executing. This behavior is not appropriate for our case because our data is lazy loaded.
- [ ] multifiles support

