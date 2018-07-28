# spark-table-fields-search

## Introduction

You may have a huge volume of data to analyse, include

1. Hundred of tables in Hive
1. Hundred of fields in a table

It’s very common to do correlation by joining different tables when analysing. The joining condition is a tuple of fields. In order to finish the analyse as soon as possible, you need to check which tables contains some special fields quickly.

This tool enables you to search tables which match filed patterns.

## Usage

Command help

```
$ spark-submit --class FieldsSearcher fields-search.jar -h
usage: Tools
 -database <arg>   database
 -fields <arg>     fields separated by ','
 -h,--help         print help information
 -recursive        search 'struct' type fields recursively
 -regex            regex mode
```

Search with plain text match

```
$ spark-submit --class FieldsSearcher fields-search.jar -database test -fields "click,impression"
```

Search with regex match

```
$ spark-submit --class FieldsSearcher fields-search.jar -database test -regex -fields ".*click.*,.*impression.*"
```

Search with regex and recursive match

```
$ spark-submit --class FieldsSearcher fields-search.jar -database test -regex -recursive -fields ".*click.*,.*impression.*"
```
