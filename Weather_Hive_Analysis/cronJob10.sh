#!/bin/sh
crontab -e

10 * * * * /home/cloudera/Downloads ./weather1.sh

#crontab -r #to delete the crontab
