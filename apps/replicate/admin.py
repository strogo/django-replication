from django.contrib import admin
from django import forms
from django.utils.translation import ugettext_lazy as _

from replicate.models import Host, Database, Conduit, Conduit_Set, Schedule, Log
from utils import execute_conduit_manually, execute_schedule, execute_conduit_set

#http://www.bromer.eu/2009/05/23/a-generic-copyclone-action-for-django-11/
from django.db.models.fields import CharField
def clone_objects(objects, title_fieldnames):
    def clone(from_object, title_fieldnames):
        args = dict([(fld.name, getattr(from_object, fld.name))
                for fld in from_object._meta.fields
                        if fld is not from_object._meta.pk]);

        for field in from_object._meta.fields:
            if field.name in title_fieldnames:
                if isinstance(field, CharField):
                    args[field.name] = getattr(from_object, field.name) + (" (%s) " % unicode(_(u'copy')))

        return from_object.__class__.objects.create(**args)

    if not hasattr(objects,'__iter__'):
       objects = [ objects ]

    # We always have the objects in a list now
    objs = []
    for object in objects:
        obj = clone(object, title_fieldnames)
        obj.save()
        objs.append(obj)
   

class Conduit_SetAdmin(admin.ModelAdmin):
    list_display = ['setname', 'concurrent', 'get_conduits']
    filter_horizontal = ('conduits',)
    order = 3
    actions = ['execute']

    def execute(self, request, queryset):
        for i in queryset:
            execute_conduit_set(i)
        
        if queryset.count() == 1:
            message_bit = _(u"1 conduit set was")
        else:
            message_bit = _(u"%s conduit sets were") % queryset.count()
        self.message_user(request, _(u"%s manually executed.  Check log file for results.") % message_bit)
    execute.short_description = _(u"Manually execute conduit set")		


class LogAdmin(admin.ModelAdmin):
    date_hierarchy = 'timestamp'
    list_filter = ['severity', 'module']
    list_display = ['id', 'timestamp', 'module', 'severity', 'message']
    search_fields = ['message']
    search_fields_verbose = ['Message']
    #list_display_links = ()
    readonly_fields = ['severity', 'module', 'message']
    order = 5
    actions = None

    
class ScheduleAdmin(admin.ModelAdmin):
    list_display = ['conduit_set', 'enabled', 'minute', 'hours', 'day_of_month', 'month', 'day_of_week', 'last_run', 'executing']
    list_display_links = list_display	
    order = 4
    actions = ['execute', 'enable', 'disable']

    def execute(self, request, queryset):
        for i in queryset:
            execute_schedule(i)
        
        if queryset.count() == 1:
            message_bit = _(u"1 schedule was")
        else:
            message_bit = _(u"%s schedules were") % queryset.count()
        self.message_user(request, _(u"%s manually executed.  Check log file for results.") % message_bit)
    execute.short_description = _(u"Manually execute schedule")	

    def enable(self, request, queryset):
        for i in queryset:
            i.enabled = True
            i.save()
        
        if queryset.count() == 1:
            message_bit = _(u"1 conduit was")
        else:
            message_bit = _(u"%s conduits were") % queryset.count()
        self.message_user(request, _(u"%s enabled.") % message_bit)
    enable.short_description = _(u"Enable schedule")

    def disable(self, request, queryset):
        for i in queryset:
            i.enabled = False
            i.save()
        
        if queryset.count() == 1:
            message_bit = _(u"1 conduit was")
        else:
            message_bit = _(u"%s conduits were") % queryset.count()
        self.message_user(request, _(u"%s disabled.") % message_bit)
    disable.short_description = _(u"Disable schedule")

    
class HostAdmin(admin.ModelAdmin):
    list_display = ['name', 'ip_address']
    list_display_links = list_display
    order = 0	

    
class DatabaseAdmin(admin.ModelAdmin):
    list_display = ['name', 'host', 'backend']
    list_display_links = ['name']
    order = 1

    
class ConduitAdmin(admin.ModelAdmin):
    search_fields = ['name']
    search_fields_verbose = ['Name']
    radio_fields = { 'type': admin.VERTICAL, 'primary_key_source': admin.HORIZONTAL }
    list_display = ['name', 'master_db', 'master_table', 'slave_db', 'slave_table', 'get_conduit_sets', 'is_enabled']
    fieldsets = (
        (None, {
            'fields': ('name', 'type', 'master_db', 'slave_db', 'master_table', 'master_subset', 'slave_table', 'slave_subset', 'primary_key_source', 'detect_primary_key', 'key_fields', 'fields_to_fetch', 'dry_run')
        }),
        (_(u'Error handling'), {
            'classes': ('collapse-closed',),
            'fields': ('ignore_slave_modify_errors', 'slave_warnings_abort_threshold', 'ignore_master_pull_errors', 'master_warnings_abort_threshold')
        }),
        (_(u'Performance'), {
            'classes': ('collapse-closed',),
            'fields': ('master_key_batchsize', 'slave_key_batchsize', 'batchsize')
        }),
        (_(u'Timeouts'), {
            'classes': ('collapse-closed',),
            'fields': ('timeout', 'major_timeout', 'minor_timeout')
        }),
    )
    order = 2	
    
    actions = ['execute', 'clone']

    def execute(self, request, queryset):
        for i in queryset:
            execute_conduit_manually(i)
        
        if queryset.count() == 1:
            message_bit = "1 conduit was"
        else:
            message_bit = "%s conduits were" % queryset.count()
        self.message_user(request, "%s manually executed.  Check log file for results." % message_bit)
    execute.short_description = _(u"Manually execute conduit")
    
    def clone(self, request, queryset):
        clone_objects(queryset, ('name',))

        if queryset.count() == 1:
            message_bit = _(u"1 conduit was")
        else:
            message_bit = _(u"%s conduits were") % queryset.count()
        self.message_user(request, _(u"%s copied.") % message_bit)
        
    clone.short_description = _(u"Copy the selected object")	


admin.site.register(Host, HostAdmin)
admin.site.register(Database, DatabaseAdmin)
admin.site.register(Conduit, ConduitAdmin)
admin.site.register(Conduit_Set, Conduit_SetAdmin)
admin.site.register(Schedule, ScheduleAdmin)
admin.site.register(Log, LogAdmin)

