# crontab-jobs
a program is able to run as crontab and process the new files that are not yet handled before.

1, use Node.Js to do parser and store pg operation, all pg connection security will be configed in config.ini file.

2, use pino do logger, per day and log any success and failed rec.

3, will use PM2 to handle stable thread in Linux env

4, will use crob function , that can be configed with simple config.ini file


any new coming changes, you don't need to change codes, just update the config.ini is enough.

the config.ini will includes

1,the path for director

2, db connection info , include address, table, username, passwd etc...

3, corb you can change the scedule base on your needs. 
