# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import random
import time
import sys
import iothub_client
import json
import os
# pylint: disable=E0611
from iothub_client import IoTHubModuleClient, IoTHubClientError, IoTHubTransportProvider, DeviceMethodReturnValue
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubModuleClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

# global counters
SEND_CALLBACKS = 0

TEMP_THRESHOLD_PROPERTY_NAME = "TemperatureThreshold"
TEMPERATURE_THRESHOLD = 25
HEART_BEAT = "heartbeat"
DESIRED_PROPERTY_KEY = "desired"

# Choose HTTP, AMQP or MQTT as transport protocol.  Currently only MQTT is supported.
PROTOCOL = IoTHubTransportProvider.MQTT

# Callback received when the message that we're forwarding is processed.


def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print("Confirmation[%d] received for message with result = %s" % (
        user_context, result))
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print("    Properties: %s" % key_value_pair)
    SEND_CALLBACKS += 1
    print("    Total calls confirmed: %d" % SEND_CALLBACKS)

# receive_message_callback is invoked when an incoming message arrives on the specified
# input queue (in the case of this sample, "input1").  Because this is a filter module,
# we will forward this message onto the "output1" queue.


def receive_message_callback(message, hubManager):
    try:
        filtered_message = filter_message(message)

        if filtered_message:
            hubManager.forward_event_to_output("output1", filtered_message, 0)
        return IoTHubMessageDispositionResult.ACCEPTED
    except Exception as e:
        print("Error when filter message: %s" % e)
        return IoTHubMessageDispositionResult.ABANDONED


def filter_message(message):
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    message_str = message_buffer[:size].decode('utf-8')
    if not message_str:
        return None

    message_obj = json.loads(message_str)
    if message_obj["machine"]["temperature"] > TEMPERATURE_THRESHOLD:
        print(" Machine temperature : %d exceeds threshold %d" %
              (message_obj["machine"]["temperature"], TEMPERATURE_THRESHOLD))
        filtered_message = IoTHubMessage(message_str)

        filtered_message.set_content_type_system_property(
            message.get_content_type_system_property() or "application/json")
        filtered_message.set_content_encoding_system_property(
            message.get_content_encoding_system_property() or "utf-8")

        prop_map = filtered_message.properties()

        origin_prop_key_value_pair = message.properties().get_internals()
        for key in origin_prop_key_value_pair:
            prop_map.add(key, origin_prop_key_value_pair[key])

        prop_map.add("MessageType", "Alert")

        return filtered_message
    else:
        return None


def module_twin_callback(update_state, payload, user_context):
    global TEMPERATURE_THRESHOLD

    properties_obj = json.loads(payload)

    if DESIRED_PROPERTY_KEY in properties_obj:
        properties_obj = properties_obj[DESIRED_PROPERTY_KEY]

    if TEMP_THRESHOLD_PROPERTY_NAME in properties_obj:
        TEMPERATURE_THRESHOLD = properties_obj[TEMP_THRESHOLD_PROPERTY_NAME]


def module_method_callback(method_name, payload, hubManager):
    print("Received method [%s]" % (method_name))
    message_str = "Module [FilterModule] is Running"
    heart_beat_messsage = IoTHubMessage(message_str)
    prop_map = heart_beat_messsage.properties()
    prop_map.add("MessageType", HEART_BEAT)
    hubManager.forward_event_to_output(HEART_BEAT, heart_beat_messsage, 0)
    print("Sent method response via event [%s]" % HEART_BEAT)

    device_method_return_value = DeviceMethodReturnValue()
    device_method_return_value.status = 200
    device_method_return_value.response = "{ \"Response\": \"This is the response from the device\" }"
    return device_method_return_value


class HubManager(object):

    def __init__(
            self,
            protocol=IoTHubTransportProvider.MQTT):
        self.client_protocol = protocol
        self.client = IoTHubModuleClient()
        self.client.create_from_environment(protocol)

        # set the time until a message times out
        self.client.set_option("messageTimeout", MESSAGE_TIMEOUT)

        # sets the callback when a message arrives on "input1" queue.  Messages sent to
        # other inputs or to the default will be silently discarded.
        self.client.set_message_callback(
            "input1", receive_message_callback, self)

        self.client.set_module_twin_callback(module_twin_callback, 0)
        self.client.set_module_method_callback(
            module_method_callback, self)

    # Forwards the message received onto the next stage in the process.
    def forward_event_to_output(self, outputQueueName, event, send_context):
        self.client.send_event_async(
            outputQueueName, event, send_confirmation_callback, send_context)


def main(protocol):
    try:
        print("\nPython %s\n" % sys.version)
        print("IoT Hub Client for Python")

        hub_manager = HubManager(protocol)

        print("Starting the IoT Hub Python sample using protocol %s..." %
              hub_manager.client_protocol)
        print("The sample is now waiting for messages and will indefinitely.  Press Ctrl-C to exit. ")

        while True:
            time.sleep(1)

    except IoTHubError as iothub_error:
        print("Unexpected error %s from IoTHub" % iothub_error)
        return
    except KeyboardInterrupt:
        print("IoTHubModuleClient sample stopped")


if __name__ == '__main__':
    main(PROTOCOL)
