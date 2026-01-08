# WhatsApp-Db-Merger

This script uses Linux so you will need a Linux environment to be able to run it.

pass the databases to merge as arguments.
Eg
./whats_db_merge.sh "/home/msgstore1.db" "/home/msgstore2.db" "/home/msgstore3.db

Original databases won't be altered

Make sure the databases have a recent schema. ie schemas beginning from 2022 and beyond.

This script intelligently checks for common columns and tables effectively avoiding potential column / table mismatch.

