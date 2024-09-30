# WhatsApp-Db-Merger

This script uses Linux so you will need a Linux environment to be able to run it.

Make sure to put the full paths to the databases inside the dbs variable first. like this: 

dbs=("/home/msgstore1.db" "/home/msgstore2.db" "/home/msgstore3.db")

Original databases won't be altered

Make sure the databases have a recent schema. ie schemas beginning from 2022 and beyond.

This script intelligently checks for common columns and tables effectively avoiding potential column / table mismatch.

