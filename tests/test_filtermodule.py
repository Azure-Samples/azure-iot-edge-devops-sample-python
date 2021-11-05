import sys, os
testdir = os.path.dirname(__file__)
srcdir = '../modules/FilterModule'
sys.path.insert(0, os.path.abspath(os.path.join(testdir, srcdir)))

import unittest
import datetime
from azure.iot.device import Message
from main import filter_message

class TestFilterModule(unittest.TestCase):
    def test_filter_less_than_threshold(self):
        source = create_message(25 - 1)
        result = filter_message(source)
        self.assertIsNone(result)
    
    def test_filter_more_than_threshold_alert_property_test(self):
        source = create_message(25 + 1)
        result = filter_message(source)
        self.assertEqual("Alert", result.custom_properties["MessageType"])

    def test_filter_more_then_threshold_copy_property(self):
        expected = "customTestValue"
        source = create_message(25 + 1)
        prop_map = source.custom_properties
        prop_map["customTestKey"] = expected
        result = filter_message(source)
        self.assertEqual(expected, result.custom_properties["customTestKey"])

def create_message(temperature):
    message_str = '{"machine":{"temperature":%s,"pressure":0}, "ambient":{"temperature":0,"humidity":0},"timeCreated":"%s"}' % (temperature, datetime.datetime.now())
    message = Message(bytearray(message_str, 'utf8'))
    return message