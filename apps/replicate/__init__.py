#http://code.activestate.com/recipes/496800/ - James Kassemi

import thread
import threading
import datetime
#from multiprocessing import Process

from django.conf import settings
from django.db import connection as django_connection

from utils import execute_schedule
from models import Schedule

from debug import debug

DEFAULT_REPLICATE_CHECKSCHEDULES_FREQUENCY = 35

class Operation(threading._Timer):
    def __init__(self, *args, **kwargs):
        threading._Timer.__init__(self, *args, **kwargs)
        self.setDaemon(True)

    def run(self):
        while True:
            self.finished.clear()
            self.finished.wait(self.interval)
            if not self.finished.isSet():
                self.function(*self.args, **self.kwargs)
            else:
                return
            self.finished.set()


class Manager(object):
    ops = []

    def add_operation(self, operation, interval, args=[], kwargs={}):
        op = Operation(interval, operation, args, kwargs)
        self.ops.append(op)
        thread.start_new_thread(op.run, ())

    def stop(self):
        for op in self.ops:
            op.cancel()
#		self._event.set()


def atoi_list(str):
    result = []
    for i in str.split(','):
        try:
            result.append(int(i))
        except:
            pass
    
    return result

def checkSchedules():
    django_connection.close() 

    ct = datetime.datetime.now()
    debug("checkSchedule: %s" % ct)
    sch_p_l = []
    
    for schedule in Schedule.objects.all():
        if schedule.enabled:
            if not schedule.executing:
                if ct.minute in atoi_list(schedule.minute) or schedule.minute == '*':
                    if ct.hour in atoi_list(schedule.hours) or schedule.hours == '*':
                        if ct.weekday() in atoi_list(schedule.day_of_week) or schedule.day_of_week == '*':
                            if ct.month in atoi_list(schedule.month) or schedule.month == '*':
                                if ct.day in atoi_list(schedule.day_of_month) or schedule.day_of_month == '*':
                                    sch_p = Process(target=execute_schedule, args=(schedule,))
                                    sch_p_l.append(sch_p)
                                    sch_p.start()
                                
debug("replicate.init")
try:
    import socket
    s = socket.socket()
    host = socket.gethostname()
    port = getattr(settings, "REPLICATE_SAFETY_PORT", 21451)
    s.bind((host, port))

    Schedule.objects.all().update(executing = False)
    timer = Manager()
    timer.add_operation(checkSchedules, getattr(settings, "REPLICATE_CHECKSCHEDULES_FREQUENCY", DEFAULT_REPLICATE_CHECKSCHEDULES_FREQUENCY))
    debug("replicate.start-ok")
except:
    debug("replicate.start-fail")
    pass

#VERSION = (0, 1)
#
# Dynamically calculate the version based on VERSION tuple
#if len(VERSION)>2 and VERSION[2] is not None:
#	str_version = "%d.%d_%s" % VERSION[:3]
#else:
#	str_version = "%d.%d" % VERSION[:2]
#
#__version__ = str_version

#import replicate
#replicate.begin()

