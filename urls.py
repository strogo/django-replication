from django.conf.urls.defaults import *
from django.contrib import admin

from grappelli.sites import GrappelliSite

#admin.site = GrappelliSite()
admin.autodiscover()
admin.site.apps = [
	{
		'name': 'Replication',
		'classes': '',
		'show_apps': False,
		'apps': ['replicate']
	},
	{
		'name': 'User Management',
		'classes': '',
		'show_apps': False,
		'apps': ['auth', 'user']
	},
	{
		'name': 'Grappelli',
		'classes': 'collapse-closed',
		'show_apps': False,
		'apps': ['grappelli']
	},
	{
		'name': 'Sites',
		'classes': 'collapse-closed',
		'show_apps': False,
		'apps': ['sites']
	}
]

urlpatterns = patterns('',
	(r'^', include(admin.site.urls)),
	(r'^grappelli/', include('grappelli.urls')),
)
