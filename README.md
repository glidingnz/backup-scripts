# Wordpress backup scripts
GNZ owns several Wordpress sites, and these scripts are used to back each of them up.


## Installation
1. Setup new Bucket in Backblaze B2
2. Create new application key for that bucket, write only
3. Create lifecycle policy for that bucket to delete backups after 90 days
4. Install B2 CLI on server
5. Setup my.cnf from my.cnf.example
6. Setup .env from .env.example
7. Setup cronjob to run backup.sh every day
