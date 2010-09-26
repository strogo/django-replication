#!/bin/sh
if [ -n "$1" ]; then
	./manage.py runserver $1 --adminmedia ./site_media/media/
else
	./manage.py runserver --adminmedia ./site_media/media/
fi
