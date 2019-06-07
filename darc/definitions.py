#!/usr/bin/env python

import os
import astropy.units as u

# config
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(ROOT_DIR, 'config.yaml')

# hosts
#MASTER = "arts041"
#WORKERS = ["arts001", "arts002", "arts003", "arts004", "arts005", 
#           "arts006", "arts007", "arts008", "arts009", "arts010", 
#           "arts011", "arts012", "arts013", "arts014", "arts015", 
#           "arts016", "arts017", "arts018", "arts019", "arts020", 
#           "arts021", "arts022", "arts023", "arts024", "arts025", 
#           "arts026", "arts027", "arts028", "arts029", "arts030", 
#           "arts031", "arts032", "arts033", "arts034", "arts035", 
#           "arts036", "arts037", "arts038", "arts039", "arts040"]
MASTER = 'bla'
WORKERS = ['arts041']

# WSRT/Apertif constants
NCHAN = 1536
BANDWIDTH = 300.*u.MHz
TSAMP = 81.92E-6*u.s
WSRT_LAT = 52.915184*u.deg
WSRT_LON = 6.60387*u.deg
WSRT_ALT = 16*u.m
NUMCB = 40
