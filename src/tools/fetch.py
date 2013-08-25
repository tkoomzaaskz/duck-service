#!/usr/bin/env python
import urllib
import simplejson

API_URL = "http://finances.ad9bis.newrails.pl/api.php"

outcomes = simplejson.load(urllib.urlopen(API_URL + "/outcome"))
incomes = simplejson.load(urllib.urlopen(API_URL + "/income"))
print type(outcomes), type(incomes)