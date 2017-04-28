# OnlineSchemaChange
OnlineSchemaChange is a tool for making schema changes for MySQL tables in a non-blocking way

## Examples

`OSC` must be run on the same host as MySQL server. 

### `copy` mode 

Say we have an existing table named `my_table` under database `test`:
```
CREATE TABLE `my_table` (
  `id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
``` 
Now if we want to run `ALTER TABLE ``my_table`` add column ``data`` varchar(10);` against it. Instead of feed the `ALTER TABLE` statement to OSC, we just need to put a `CREATE TABLE` statement representing the desired schema into a file `/tmp/foo.sql` like below:
```
CREATE TABLE `my_table` (
  `id` int(11) NOT NULL,
  `data` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
```
Then run the following command:
```
osc_cli copy --ddl-file-list=foo.sql --socket=/tmp/mysql.socket --database=test
```

### `cleanup` mode

To cleanup the table left behind by last run of OSC
```
osc_cli cleanup --socket=/tmp/mysql.socket --database=test
```

To terminate a currently running `OSC` process on certain MySQL Instance:
```
osc_cli cleanup
```

### `direct` mode

This mode is reserved for utilizing MySQL's native online ddl for schema change, and help DBA manage all the schema related operation into this one tool.
For now, if you plan to use `OSC` for all your schema management, this mode is here for you to create a empty new table. Following command will create an empty table into database `test`
```
osc_cli direct --ddl-file-list=foo.sql --socket=/tmp/mysql.socket --database=test
```


## Requirements
OnlineSchemaChange requires 

**System packages**

For Python2
```
sudo apt-get install python-dev libmysqlclient-dev # Debian / Ubuntu
sudo yum install python-devel mysql-devel # Red Hat / CentOS
```

For Python3
```
sudo apt-get install python3-dev # debian / Ubuntu
sudo yum install python3-devel # Red Hat / CentOS
```

**Python requirements**
* python >= 2.7
* python module: six, [pyparsing](http://pyparsing.wikispaces.com/), [MySQLdb](http://github.com/PyMySQL/mysqlclient-python/tarball/master)

## Installing OnlineSchemaChange

Run following command to install dependency
```
python setup.py install --install-scripts=/usr/local/bin
```
If you have multiple python version available in your environment, or you don't want mess up with system's default python, 
you can use `pyenv` and `virtualenv`

## How OnlineSchemaChange works

Check wiki page for more detail, and some advanced usage.

## How to contribute
Check this [wiki page](https://github.com/facebookincubator/OnlineSchemaChange/blob/master/CONTRIBUTING.md) if you want to contribute to this project.


## License
OnlineSchemaChange is BSD-licensed. We also provide an additional patent grant.


