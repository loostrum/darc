#!/usr/bin/env python3

import os
import astropy.units as u

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# try to find config file in home directory
config_file = os.path.join(os.path.expanduser('~'), 'darc', 'config.yaml')
if not os.path.isfile(config_file):
    # switch to default config file
    config_file = os.path.join(ROOT_DIR, 'config.yaml')
#: Full path to config file: $HOME/darc/config.yaml if present, else default shipped with package
CONFIG_FILE = config_file

#: ARTS master name
MASTER = "arts041"
MASTER = "zeus"

#: ARTS worker names
WORKERS = ["arts001", "arts002", "arts003", "arts004", "arts005",
           "arts006", "arts007", "arts008", "arts009", "arts010",
           "arts011", "arts012", "arts013", "arts014", "arts015",
           "arts016", "arts017", "arts018", "arts019", "arts020",
           "arts021", "arts022", "arts023", "arts024", "arts025",
           "arts026", "arts027", "arts028", "arts029", "arts030",
           "arts031", "arts032", "arts033", "arts034", "arts035",
           "arts036", "arts037", "arts038", "arts039", "arts040"]

#: WSRT latitude
WSRT_LAT = 52.915184 * u.deg

#: WSRT longitude
WSRT_LON = 6.60387 * u.deg

#: WSRT altitude
WSRT_ALT = 16 * u.m

#: WSRT dish diameter
DISH_DIAM = 25 * u.m

#: Apertif/ARTS number of channels
NCHAN = 1536

#: Apertif/ARTS bandwidth
BANDWIDTH = 300. * u.MHz

#: Apertif/ARTS sampling time
TSAMP = 81.92E-6 * u.s

#: Apertif/ARTS number of compound beams
NUMCB = 40

#: Apertif/ARTS system temperature
TSYS = 85 * u.Kelvin

#: Apertif/ARTS aperture efficiency
AP_EFF = .60

#: Apertif/ARTS number of dishes in use
NDISH = 8

#: Number of tied-array beams
NTAB = 12

#: Beamformer time constant: number of samples per second
TIME_UNIT = 781250
