## How to get Postgresql (pgsql) running.
1. Login in to the lacdrvm.
2. Move to the root directory using
cd \
3. Open the psql program. this is a cli interface for pgsql.
sudo -u sa psql
4. Check that you are logged in to the db "sa" as the user "sa".
\conninfo
To leave the psql shell at any time, use the following.
\q
5. Connect to the faers\_a db.
\c faers_a
6. It should now display that you are connected to the faers\_a db. You can list
the relations already present with
\dt
7. Just as a test, run the following query. What does it do? Notice the ; is needed
to execute the query, just like in OCaml or other languages. Also notice that
these two statements do exactly the same. As a convention, we will be using
uppercase, but for quickly prototyping lowercase can be easier.
SELECT COUNT(\*) FROM drug12q4;
select count(\*) from drug12q4;
8. In order to run a file, make sure it has the correct file permissions. You can
put your scripts in the /faers-scripts folder for testing. For instance, here
I check if the script pgtest.sql has read write permissions for the postgres
user. Note that this is happening in the bash shell, not the psql shell!
sudo su postgres -g postgres -s /bin/bash -c "test -r /faers-scripts/pgtest.sql" || {echo "you do not have this perm"}
sudo su postgres -g postgres -s /bin/bash -c "test -w /faers-scripts/pgtest.sql" || {echo "you do not have this perm"}
9. If the file has the proper permissions, it should be able to run. postgres is able
to read any text files with absolute path /faers/data/file.txt . For instance, we can
now run the pgtest.sql file from within the psql shell:
\i /faers-scripts/pgtest.sql
10. You can now check if it has run correctly by rerunning the above query. Do note that
this works well because we first dropped the table (removed it), and then replaced it. Make
sure to _always_ do this! This way the db doesn't get clobbered with tons of
unnecessary tables.