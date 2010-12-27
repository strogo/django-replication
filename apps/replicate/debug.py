import sys, os

from django.conf import settings

def debug(msg):
    #print >> sys.stderr, "DEBUG: %s - %s" % (os.path.basename(__file__), msg)
    print >> sys.stderr, "DEBUG: %s" % msg
