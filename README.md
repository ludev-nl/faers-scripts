#TODO: this should contain a user guide for Laura or Roelof

# <a name="ci"></a> CI Tools

This project uses SQLFluff, ShellCheck, and Bats for static analysis and testing.

1. SQLFluff
Install it with
```
pip install sqlfluff
```
Install it with ""

Lint (Reviews code) SQL files with "sqlfluff lint path/to/sql/files"

Fix (Modifies the code) SQL files with "sqlfluff fix path/to/sql/files"

2.ShellCheck

Installing ShellCheck with

- Ubuntu "sudo apt-get install shellcheck"
- MAC OS "brew install shellcheck"
- Windows "choco install shellcheck"

Run ShellCheck with "shellcheck path/to/script.sh"

3. Bats

Install with "git clone https://github.com/bats-core/bats-core.git
cd bats-core sudo ./install.sh"

Run with "bats tests/faers_scripts_test.bats"

To verify if used properly
"sqlfluff lint path/to/sql/files

shellcheck path/to/script.sh

bats tests/faers_scripts_test.bats
"

This Continuous Integration pipeline runs when there is a pull request, the SQL scripts are checked with SQLFluff, the shell scripts with ShellCheck, and the Bats functionality with Bats, this will check for the quality and functionality of the code.


# <a name="postgresql"></a> How to get Postgresql (pgsql) running
1. Login in to the lacdrvm.
2. Move to the root directory using
```
cd \
```
3. Open the psql program. this is a cli interface for pgsql.
```
sudo -u sa psql
```
4. Check that you are logged in to the db "sa" as the user "sa".
```
\conninfo
```
To leave the psql shell at any time, use the following.
```
\q
```
6. Connect to the faers\_a db.
```
\c faers_a
```
8. It should now display that you are connected to the faers\_a db. You can list
the relations already present with
```
\dt
```
10. Just as a test, run the following query. What does it do? Notice the ; is needed
to execute the query, just like in OCaml or other languages. Also notice that
these two statements do exactly the same. As a convention, we will be using
uppercase, but for quickly prototyping lowercase can be easier.
```
SELECT COUNT(\*) FROM drug12q4;
select count(\*) from drug12q4;
```
11. In order to run a file, make sure it has the correct file permissions. You can
put your scripts in the /faers-scripts folder for testing. For instance, here
I check if the script pgtest.sql has read write permissions for the postgres
user. Note that this is happening in the bash shell, not the psql shell!
```
sudo su postgres -g postgres -s /bin/bash -c "test -r /faers-scripts/pgtest.sql" || {echo "you do not have this perm"}
sudo su postgres -g postgres -s /bin/bash -c "test -w /faers-scripts/pgtest.sql" || {echo "you do not have this perm"}
```
12. If the file has the proper permissions, it should be able to run. postgres is able
to read any text files with absolute path /faers/data/file.txt . For instance, we can
now run the pgtest.sql file from within the psql shell:
```
\i faers-scripts/pgtest.sql
```
13. You can now check if it has run correctly by rerunning the above query. Do note that
this works well because we first dropped the table (removed it), and then replaced it. Make
sure to _always_ do this! This way the db doesn't get clobbered with tons of
unnecessary tables.



# <a name="py-eh-l"></a> Python error handling and logging
Our logging and fatal error handling is done in `src/error.py`.
Here we initialise and configure the logging handlers, and
handle fatal errors.

1. To import and use the logger in your file, add this to the top
of the file (make `log` a global object for convenience).
```
from error import fatal_error, get_logger
log = get_logger()
```
2. You can now add logging and messaging. The logger has been set
up to print `info` levels to `stdout` too, and store all levels including
these in a log file, stored in `faers/data/logs` by default (for now).
For example:
```
# An info message to show the program has completed a major step.
# Useful for the user.
if len(non_existent_dirs) == 0:
    log.info("Directories correctly initialised.")
    return False
```
or
```
# If there is an error but we can fall back to a default value, you
# can use something like this. This will not be shown to the user,
# but appear in the log. Most likely the program will error out later
# on anyway.
try:
    with open(dir_config_location) as f:
        config = json.load(f)
except json.JSONDecodeError as e:
    log.warning("Directory config is of an invalid JSON format")
    # Fallback: empty dictionary
    config['data_directories'] = {}
```
or
```
# If the error will disrupt the program entirely, we should exit
# with an error code. The exit code should be the script number
preferably (such as 1 here).
try:
    os.remove(self.current_zip)
except Exception as e:
    raise fatal_error("Error removing zip file", e, 1)
```

# <a name="py-unit-testing"></a> Python unit testing
This section explains how to run `pytest` for the
tests present in `tests/test_check_directories.py`, the process will be
the same for your test file if it is present in the same folder.

1. Write your tests, and store them in the `tests/` folder.
2. Move to the project root directory, and run
```
python3.11 -m venv venv
source /bin/source/activate # On Linux, may be different elsewhere
which python3.11 # Should be inside your virtual environment
pip install pytest
pip install -e . # Build our package according to our pyproject.toml
```
3. You can now run tests inside this virtual environment.
```
python3 -m pytest tests/test_check_directories.py
================================ test session starts ================================
platform linux -- Python 3.13.3, pytest-8.3.5, pluggy-1.5.0
rootdir: <project root directory>
configfile: pyproject.toml
collected 5 items

tests/test_check_directories.py .....                                         [100%]

================================= 5 passed in 0.02s =================================
```
Note that we use `python3.11` instead of `python3` because the default Python version
for `python3` on Rocky Linux is Python 3.9.