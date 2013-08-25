#!/usr/bin/python

import urllib2, json, sys

if len(sys.argv) < 2:
    raise Exception('Provide controller')
controller = sys.argv[1]
api_url = 'http://finances.ad9bis.newrails.pl/' + controller

response = urllib2.urlopen(api_url + 'users')
print json.loads(response.read())

response = urllib2.urlopen(api_url + 'income_categories')
print json.loads(response.read())

response = urllib2.urlopen(api_url + 'outcome_categories')
print json.loads(response.read())
