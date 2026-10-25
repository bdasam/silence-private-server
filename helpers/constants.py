# constant.py
LOGGER_NAME = 'Silence-Server-Logger'

VERSION = '2024.5.21'

# internal pub/sub topics
TOPIC_SOCKET = 'socket'
TOPIC_SCOOTER_STATUS = 'scooter_status'
TOPIC_COMMAND_RECEIVED = 'command_received'
TOPIC_COMMAND_RESULT = 'command_result'

# defaults MQTT topics
MQTT_STATUS = 'status'
MQTT_COMMAND = 'command'
MQTT_RESULT = 'result'

def imei_topic(base, imei):
    """Internal pub/sub topic scoped to one vehicle. pypubsub topics are
    process-wide, so with several IMEIs an unscoped topic delivered every
    command to every vehicle and every status to every MQTT prefix."""
    return base + "_" + "".join(c if c.isalnum() else "_" for c in str(imei))

# RAW_COMMAND
RAW_COMMAND_CODE = 'RAW_COMMAND'

# TCU reply text (e.g. "$FIND,OK"), published on MQTT <prefix>/<IMEI>/reply
TOPIC_COMMAND_REPLY = 'command_reply'
MQTT_REPLY = 'reply'
