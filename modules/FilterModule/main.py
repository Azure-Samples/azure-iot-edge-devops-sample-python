# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import asyncio
import json
import signal
import threading
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message, MethodResponse
from azure.iot.device.exceptions import ClientError, OperationTimeout


TEMPERATURE_THRESHOLD = 25
TEMP_THRESHOLD_PROPERTY_NAME = "TemperatureThreshold"
HEART_BEAT = "heartbeat"


def create_client():
    client = IoTHubModuleClient.create_from_edge_environment()

    # Define function for handling received messages
    async def receive_message_handler(message):
        try:
            filtered_message = filter_message(message)
        except KeyError as e:
            print("Error when filtering message: %s" % e)
        else:
            if filtered_message:
                await client.send_message_to_output(filtered_message, "output1")
                print("Forwarded message to Hub")

    def twin_patch_received_handler(twin_patch):
        """Update the TEMPERATURE THRESHOLD value if its twin property is updated"""
        global TEMPERATURE_THRESHOLD

        if TEMP_THRESHOLD_PROPERTY_NAME in twin_patch:
            TEMPERATURE_THRESHOLD = twin_patch[TEMP_THRESHOLD_PROPERTY_NAME]

    async def method_request_received_handler(method_request):
        print("Received method request [{}]".format(method_request.name))
        # Send the heart beat message to the corresponding output
        heart_beat_message = Message("Module [FilterModule] is running")
        heart_beat_message.custom_properties["MessageType"] = HEART_BEAT
        await client.send_message_to_output(HEART_BEAT, heart_beat_message)
        print("Heart beat message sent to heart beat output")

        # Send the message response 
        response_payload = "{ \"Response\": \"This is the response from the device\" }"
        method_response = MethodResponse.create_from_method_request(
            method_request=method_request, status=200, payload=response_payload)
        await client.send_method_response(method_response)
        print("Method response to [{}] sent".format(method_request.name))

    try:
        # Attach client handlers
        client.on_message_received = receive_message_handler
        client.on_twin_desired_properties_patch_received = twin_patch_received_handler
        client.on_method_request_received = method_request_received_handler
    except:
        # Clean up in the event of failure
        client.shutdown()
        raise

    return client


def filter_message(message):
    message_str = message.data.decode("utf-8)")

    if not message_str:
        return None
    
    print("RECEIVED:")
    print(message_str)
    message_obj = json.loads(message_str)
    message_temp = message_obj["machine"]["temperature"]

    if message_temp > TEMPERATURE_THRESHOLD:
        print("Machine temperature {} exceeds threshold {}".format(message_temp, TEMPERATURE_THRESHOLD))

        message.custom_properties["MessageType"] = "Alert"
        return message
    else:
        return None


def main():
    print("Filter Module")
    print("Press Ctrl-C to exit")

    # Event indicating sample stop
    stop_event = threading.Event()

    # Define a signal handler that will indicate Edge termination of the Filter Module
    def module_termination_handler(signal, frame):
        print("Filter Module stopped")
        stop_event.set

    # Attach the signal handler
    signal.signal(signal.SIGTERM, module_termination_handler)

    # Instantiate the client. Use the same instance of the client for the duration of
    # your application
    client = create_client()

    loop = asyncio.get_event_loop()
    try:
        print("Starting the Filter Module...")
        loop.run_until_complete(client.connect())
        print("The sample is now waiting for messages indefinitely")
        # This event will be triggered by Edge termination signal
        stop_event.wait()
    except ClientError as e:
        print("Unexpected client error %s" % e)
        raise
    finally:
        # Graceful exit
        print("Shutting down client")
        loop.run_until_complete(client.shutdown())
        loop.close()

if __name__ == '__main__':
    main()