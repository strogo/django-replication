from django.db import models
from django.utils.translation import ugettext_lazy as _
from django.conf import settings


def smart_truncate(content, length=100, suffix='...'):
    return content[:length].rsplit(' ', 1)[0]+suffix
#	return (content if len(content) <= length else content[:length].rsplit(' ', 1)[0]+suffix)	


class Host(models.Model):
    name = models.CharField(max_length=32, verbose_name=_(u"name"),
                        help_text=_(u"A name to identify this host."))
    ip_address = models.IPAddressField(verbose_name=_(u"IP address"))
    
    def __unicode__(self):
        return self.name

    class Meta:
        ordering = ('name',)
        verbose_name = _(u"host")
        verbose_name_plural = _(u"hosts")		
        
        
class Database(models.Model):
    host = models.ForeignKey(Host, verbose_name=_(u"host"))
    BACKEND_CHOICES = (
        ('mysql', 'MySQL'),
        ('oracle', 'ORACLE'),
    # UNTESTED
        ('postgresql', 'PostgreSQL'),
        ('postgresql_psycopg2', 'PostgreSQL psycopg V2.x'),
        ('sqlite3', 'SQLite3'),
        ('ado_mssql', 'Microsoft SQL Server'),
    # MS SQL/SYBASE
    )
    backend = models.CharField(max_length=16, choices=BACKEND_CHOICES,
                               verbose_name=_(u"backend"),
                               help_text=_(u"Database driver to be used"))
    name = models.CharField(max_length=32,
        help_text=_(u"Name of the database (or ORACLE database service name)"),
        verbose_name=_(u"name"))
    #TODO: string to dict()
    options = models.TextField(blank=True, null=True, verbose_name=_(u"options"))
    username = models.CharField(max_length=32, blank=True, null=True,
                                verbose_name=_(u"username"),
                                help_text=_(u"The username that will be used to connect to this database"))
    password = models.CharField(max_length=64, blank=True, null=True,
                                verbose_name=_(u"password"),
                                help_text=_(u"Password of the user that will be used to connect to the database"))
    port = models.CharField(max_length=8, blank=True, null=True, verbose_name=_(u"port"),
                                help_text=_(u"The port used to connect to the database, leave blank for default port number"))
    timezone = models.CharField(max_length=32, default=settings.TIME_ZONE, verbose_name=_(u"timezone"))

    def __unicode__(self):
        return "%s @ %s" % (self.name, self.host)

    class Meta:
        ordering = ('name',)
        verbose_name = _(u"database")
        verbose_name_plural = _(u"databases")
        
        
class Conduit(models.Model):
    name = models.CharField(max_length=64, verbose_name=_(u"name"))
    TYPE_CHOICES = (
        ('e1', _(u'Incremental/slave append only (no updates or deletes on slave)')),
#        ('e2', _(u'Empty/append alias Full, Snapshot  *N/A*')),
#		('m1', 'MySQL master-slave'),
    )
    type = models.CharField(max_length=2, choices=TYPE_CHOICES, verbose_name=_(u"type"))
    master_db = models.ForeignKey(Database, related_name='master_db', help_text=_(u"Database where records will be fetch."), verbose_name = _(u"master database"))
    slave_db = models.ForeignKey(Database, related_name='slave_db', help_text=_(u"Database that will receive (or update, or delete) records."), verbose_name = _(u"slave database"))
    master_table = models.CharField(max_length=32, verbose_name=_(u"master table"))
    master_subset = models.TextField(blank=True, null=True, help_text=_(u"Limits the amount of master keys compared for replication.  Also useful in fan-out style replication."), verbose_name = _(u"master subset"))
    slave_table = models.CharField(max_length=32, verbose_name=_(u"slave table"))
    #In the future may be null if slave_table_name == master_table_name
    slave_subset = models.TextField(blank=True, null=True, help_text=_(u"Limits the amount of slave keys compared for replication.  Also useful in fan-in style replication."), verbose_name = _(u"slave subset"))
    PKSRC_CHOICES = (
        ('M', _(u'Master')),
        ('S', _(u'Slave')),
    )
    primary_key_source = models.CharField(max_length=1, default='M', choices=PKSRC_CHOICES, verbose_name=_(u'primary key source'), help_text = _(u"Determines which database (master or slave) is going to be queried to determine the primary key."))
    detect_primary_key = models.BooleanField(default=True, help_text=_(u'Only works for MySQL databases so far.'), verbose_name = _(u"detect primary key"))
    key_fields = models.TextField(blank=True, null=True, help_text=_(u'Comma separated list of fields that compose a primary key.'), verbose_name = _(u"key fields"))
    master_key_batchsize = models.PositiveIntegerField(default=1000, help_text=_(u'Size of keys block to fetch from master when comparing master/slave difference (optimization value affected by: network speed/latency, computer memory amount).  Use 0 to disable this feature and fetch all keys in one request.'), verbose_name = _(u"master key fetch buffer size"))
    slave_key_batchsize = models.PositiveIntegerField(default=1000, help_text=_(u'Size of keys block to fetch from slave when comparing master/slave difference (optimization value affected by: network speed/latency, computer memory amount).  Use 0 to disable this feature and fetch all keys in one request.'), verbose_name = _(u"slave key fetch buffer size"))
    batchsize = models.PositiveIntegerField(default=1000, help_text=_(u'Amount of records to append per conduit execution (this value is independant of [master_key_buffersize] value.'), verbose_name=_(u"conduit batchsize"))
    fields_to_fetch = models.TextField(blank=True, null=True, help_text=_(u'Comma separated list of fields that will be replicated, if not specified all fields will be used.'), verbose_name=_(u"field to fetch"))
    dry_run = models.BooleanField(default=True, help_text=_(u"Don't actually modify any data only log messages"), verbose_name=_(u"dry run"))
    ignore_slave_modify_errors = models.BooleanField(default=False, help_text=_(u'Ignore situations where a single slave append query returns an error (typical of incorrect primary key fields)'), verbose_name=_(u"ignore slave modify error"))
    slave_warnings_abort_threshold = models.PositiveIntegerField(default=0, help_text=_(u"Abort conduit after this many slave warnings.  A value of zero disables this function."), verbose_name=_(u"slave warnings abort threshold"))
    #UNTESTED/NECESARY?
    ignore_master_pull_errors = models.BooleanField(default=False, help_text=_(u'Ignore situations where a single master pull query returns more than 1 or 0 rows (typical of incorrect primary key fields)'), verbose_name=_(u"ignore master pull errors"))
    master_warnings_abort_threshold = models.PositiveIntegerField(default=0, help_text=_(u"Abort conduit after this many master warnings.  A value of zero disables this function."), verbose_name=_(u"master warnings abort threshold"))

    timeout = models.PositiveIntegerField(default = 900, help_text = _(u"The maximum amount of time that this conduit is allowed to execute before it gets stopped."), verbose_name = _(u"conduit timeout"))
    minor_timeout = models.PositiveIntegerField(default = 60, help_text = _(u"Timeout (in seconds) for simple database operations, such as: Establishing connections, discovering primary keys, etc."), verbose_name = _(u"minor operations timeout"))
    major_timeout = models.PositiveIntegerField(default = 500, help_text = _(u"Timeout (in seconds) for complex database operations, such as: Fetching primary keys, fetching rows, etc."), verbose_name = _(u"major operations timeout"))
    
    #IMPROVE/IMPLEMENT
    #master_multi_row_fetch_timeout = models.PositiveIntegerField(default = 600) #10 mins
    #slave_multi_row_fetch_timeout = models.PositiveIntegerField(default = 600)  #10 mins
    

    #DELETES
    #allow_slave_deletes = models.BooleanField(default=False, verbose_name=_(u'allow slave deletes'), help_text=_(u'Delete rows on the slave database not found on the master database.')
    
    #UPDATES
    #fields to compare (between master & slave & operation) to determine a update_set
    #		- TextField => dict()
    #		- { 'update_date', 'branch_update_date', '>' }
    #		- provide %(last_replication_date)s == last date this conduit's update completed ok
    #		- provide %(last_replication_time)s == last date this conduit's update completed ok
    #		- provide %(last_replication_timestamp)s == last date this conduit's update completed ok
    
    #FULL update/snapshots
    # test = models.TextField()
    # expected_result = models.TextField()  in format = <  ['5','2']  > == single row or < [['43','34']['123','123]] == multirow

    def __unicode__(self):
        return self.name
        
    def is_enabled(self):
        return self.dry_run == False
    is_enabled.short_description = _(u'enabled')
    is_enabled.boolean = True

    def get_conduit_sets(self):
        return ', '.join(['"%s"' % c for c in self.conduit_set_set.all()])
    get_conduit_sets.short_description = _(u'conduit sets')

    def get_schedules(self):
        return ', '.join(['"%s"' % s for s in [a.schedule_set.all() for a in self.conduit_set_set.all()]])
    get_schedules.short_description = _(u'schedules')
    
#get _schedule	
    class Meta:
        ordering = ('name',)
        verbose_name = _(u"conduit")
        verbose_name_plural = _(u"conduits")		

    
class Log(models.Model):
    timestamp = models.DateTimeField(auto_now_add=True, verbose_name=_(u"timestamp"))
    module = models.CharField(max_length=32, blank=True, null=True, verbose_name=_(u"module"))
    severity = models.CharField(max_length=16, verbose_name=_(u"severity"))
    message = models.TextField(verbose_name=_(u"message"))

    def __unicode__(self):
        return "%s | %s | %s | %s" % (self.timestamp, self.module, self.severity, smart_truncate(self.message, 80))
        
    class Meta:
        ordering = ('-id',)
        verbose_name = _(u"log")
        verbose_name_plural = _(u"logs")		
    
    
class Conduit_Set(models.Model):
    setname = models.CharField(max_length=64, verbose_name=_(u"set name"))
    concurrent = models.BooleanField(default=False,
            help_text=_(u"If true, allows the conduits contained within to execute at the same time else sequencially."),
                                    verbose_name=_(u"concurrent"))
    conduits = models.ManyToManyField(Conduit, verbose_name=_(u"conduits"))

    #transaction_size(1=single, >1 = bulk)
    #abort_set_on_conduit_error = If one conduit fails abort the entire conduit_set
    
    def __unicode__(self):
        return self.setname

    class Meta:
        ordering = ('setname',)
        verbose_name = _(u"conduit set")
        verbose_name_plural = _(u"conduit sets")
        
    def get_conduits(self):
        return ', '.join(['"%s"' % c.name for c in self.conduits.all()])
    get_conduits.short_description = _(u'conduits')
    
    
class Schedule(models.Model):
    #TODO: Research schedule & Conduit_set may be merged?
    conduit_set = models.ForeignKey(Conduit_Set, verbose_name=_(u"conduit set"), help_text=_(u'Select which conduit set to schedule.'))
    enabled = models.BooleanField(default=True, verbose_name=_(u"enabled"))
    minute = models.CharField(max_length=32, default='0,15,30,45', verbose_name=_(u"minute"), help_text=_(u'At which minute(s) the schedule should run.  For multiple entries with a comma.'))
    hours = models.CharField(max_length=32, default='*', verbose_name=_(u"hours"), help_text=_(u'At which hour(s) the schedule should run.  For multiple entries with a comma.'))
    day_of_month = models.CharField(max_length=32, default='*', verbose_name=_(u"day of the month"), help_text=_(u'At which day(s) of the month the schedule should run.  For multiple entries with a comma.'))
    month = models.CharField(max_length=32, default='*', verbose_name=_(u"month"), help_text=_(u'At which month(s) the schedule should run.  For multiple entries with a comma.'))
    day_of_week = models.CharField(max_length=32, default='*', verbose_name=_(u"day of the week"), help_text=_(u'At which day(s) of the week the schedule should run.  For multiple entries with a comma.'))
    last_run = models.DateTimeField(blank=True, null=True, editable=False, verbose_name=_(u"last ran"))
    executing = models.BooleanField(editable=False, verbose_name=_(u"executing?"))

    def __unicode__(self):
        output = "%s @ %s %s %s %s %s [%s]" % (self.conduit_set, self.minute, self.hours, self.day_of_month, self.month, self.day_of_week, self.enabled and 'X' or ' ')
        return output
        
    def month_name(self):
        months = {
            u'1': _(u"January"),
            u'2': _(u"Febraury"),
            u'3': _(u"March"),
            u'4': _(u"April"),
            u'5': _(u"May"),
            u'6': _(u"June"),
            u'7': _(u"July"),
            u'8': _(u"August"),
            u'9': _(u"September"),
            u'10': _(u"October"),
            u'11': _(u"November"),
            u'12': _(u"December"),
            u'*': _(u"All"),
        }
        return ', '.join([unicode(months[m]) for m in self.month.split(',')])
    month_name.short_description = _(u'month')
        
    class Meta:
        ordering = ('conduit_set',)
        verbose_name = _(u"schedule")
        verbose_name_plural = _(u"schedules")
