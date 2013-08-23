import os, ConfigParser, MySQLdb

c = ConfigParser.ConfigParser()
c.read(os.path.abspath('./config/config.ini'))

db = MySQLdb.connect(host=c.get('mysql','host'),
    user=c.get('mysql','username'),
    passwd=c.get('mysql','password'),
    db=c.get('mysql','database'))

path = c.get('paths', 'dump')
