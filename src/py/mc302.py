#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Copyright (C) 2012, Robert Johansson <rob@raditex.nu>, Raditex Control AB
# All rights reserved.
# ------------------------------------------------------------------------------

"""
mbus test: send a request frame and receive and parse the reply
"""

from mbus.MBus import MBus
#import xmltodict

debug = True
mbus_addresses = [1,15,31] # m-bus primary addresses to request data from

mbus = MBus(device=b'/dev/ttyUSB0')	# C-library expects byte strings, Python3 string is unicode -> prefix string with 'b'

#reply_dict = {}
reply_xml = {}
try:
	mbus.connect()
	for adr in mbus_addresses:
		try:
			mbus.send_request_frame(adr)
			reply_xml[adr] = mbus.frame_data_xml(mbus.frame_data_parse(mbus.recv_frame()))
			#reply_dict[adr] = xmltodict.parse(mbus.frame_data_xml(mbus.frame_data_parse(mbus.recv_frame()))) # xmltodict works directly on the byte-string xml from libmbus
		except Exception as e:
			print ("Exception while requesting data from m-bus : type %s, desc: %s" % (type(e),str(e)))
			pass
	mbus.disconnect()
except Exception as e:
	print ("Exception while requesting data from m-bus : type %s, desc: %s" % (type(e),str(e)))
	pass

# if reply_dict:
# 	for k,resp in enumerate(reply_dict):
# 		print("i: %s key:%s value:\n%s" % (k,resp,reply_dict[resp]))
if reply_xml:
 	for k,resp in enumerate(reply_xml):
 		print("i: %s key:%s value:\n%s" % (k,resp,reply_xml[resp]))



