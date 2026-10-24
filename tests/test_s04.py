"""Tests for the s04 branch changes.

- per-IMEI internal pub/sub topics (no command/status cross-talk between vehicles)
- TCPServer.extendedCanPolling disables the hard-coded $RCAN poll burst
- TCPServer.singlePort: one listening port, connections routed by login IMEI

Only loopback sockets are used; bridgeMode is always off.
"""
import socket
import threading
import time
from types import SimpleNamespace

import pytest
from pubsub import pub

from helpers.constants import *
from helpers.messageParser import MessageParser
from services.CommandService import CommandService
from services.MQTTService import MQTTService
from services.SilenceServerService import SilenceServerService
from services.SinglePortRouter import SinglePortRouter, mask_imei

# Fake IMEIs: never real vehicle identifiers
IMEI_A = "860000000000011"
IMEI_B = "860000000000022"

MQTT_CONFIG = {
    "MQTTbroker": "127.0.0.1", "MQTTport": 1, "MQTTuser": "", "MQTTpass": "",
    "TopicPrefix": "home/silence-server",
}


def login_frame(imei):
    return f"$ASTRA;AT402;{imei};;7.0.61.35;Z;0\r\n".encode()


def z_frame(status):
    frame = bytearray(94)
    frame[0] = 0x5A
    frame[1] = 0x00
    frame[2] = 0x5E
    frame[3] = 0x01
    frame[82] = status
    return bytes(frame)


def server_config(**overrides):
    config = {
        "serverPORT": None, "bridgeMode": False,
        "silenceHOST": "127.0.0.1", "silencePORT": 1,
        "SaveToDB": False, "keepAliveInterval": 0,
        "BMScellVoltage_pooling_interval": 600,
        "extendedCanPolling": False,
    }
    config.update(overrides)
    return config


def read_for(sock, seconds):
    """Everything the server sends within `seconds`."""
    sock.settimeout(0.1)
    data = b""
    end = time.time() + seconds
    while time.time() < end:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        except socket.timeout:
            continue
    return data


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Per-IMEI topics
# ---------------------------------------------------------------------------

def test_imei_topic_is_scoped_and_valid():
    assert imei_topic(TOPIC_COMMAND_RECEIVED, IMEI_A) != imei_topic(TOPIC_COMMAND_RECEIVED, IMEI_B)
    # placeholder IMEIs from the upstream template contain '-'
    assert imei_topic(TOPIC_SOCKET, "my_imei-1") == "socket_my_imei_1"


def test_command_for_one_imei_is_queued_only_for_that_imei():
    service_a = CommandService({}, IMEI_A)
    service_b = CommandService({}, IMEI_B)
    service_a.start()
    service_b.start()
    mqtt_a = MQTTService(MQTT_CONFIG, IMEI_A)

    message = SimpleNamespace(
        topic=f"home/silence-server/{IMEI_A}/command/FLASH", payload=b"")
    mqtt_a.on_message(None, None, message)

    assert service_a.command_queue.qsize() == 1
    assert service_b.command_queue.qsize() == 0
    service_a.stop()
    service_b.stop()


def test_status_of_one_imei_is_not_published_for_another():
    received = {IMEI_A: [], IMEI_B: []}

    def listener_a(scooter_status):
        received[IMEI_A].append(scooter_status)

    def listener_b(scooter_status):
        received[IMEI_B].append(scooter_status)

    pub.subscribe(listener_a, imei_topic(TOPIC_SCOOTER_STATUS, IMEI_A))
    pub.subscribe(listener_b, imei_topic(TOPIC_SCOOTER_STATUS, IMEI_B))
    try:
        MessageParser(IMEI_B).parse_message_from_scooter_protocol_Z(z_frame(4))
        assert received[IMEI_A] == []
        assert len(received[IMEI_B]) == 1
    finally:
        pub.unsubscribe(listener_a, imei_topic(TOPIC_SCOOTER_STATUS, IMEI_A))
        pub.unsubscribe(listener_b, imei_topic(TOPIC_SCOOTER_STATUS, IMEI_B))


def test_command_result_is_scoped_to_the_imei():
    results = []

    def listener(command, result):
        results.append(command)

    pub.subscribe(listener, imei_topic(TOPIC_COMMAND_RESULT, IMEI_A))
    try:
        service_b = CommandService({}, IMEI_B)
        service_b.commands_definition = {}
        service_b.command_received("NOPE", "")   # publishes "Invalid command" for B
        assert results == []
    finally:
        pub.unsubscribe(listener, imei_topic(TOPIC_COMMAND_RESULT, IMEI_A))


# ---------------------------------------------------------------------------
# extendedCanPolling
# ---------------------------------------------------------------------------

def run_session(config, imei=IMEI_A):
    """Log in over a socketpair, report status=on, return what the server sent."""
    service = SilenceServerService(config, imei)
    vehicle, server_side = socket.socketpair()
    thread = threading.Thread(target=service.handle_client, args=(server_side,), daemon=True)
    thread.start()
    vehicle.sendall(login_frame(imei))
    ack = read_for(vehicle, 0.5)
    vehicle.sendall(z_frame(4))
    sent = read_for(vehicle, 1.0)
    vehicle.close()
    thread.join(3)
    service.stop()
    return ack, sent


def test_extended_can_polling_off_sends_no_rcan():
    ack, sent = run_session(server_config(extendedCanPolling=False))
    assert ack == b"\x06"
    assert b"$RCAN" not in sent


def test_extended_can_polling_on_keeps_upstream_behaviour():
    ack, sent = run_session(server_config(extendedCanPolling=True))
    assert ack == b"\x06"
    assert b"$RCAN,185" in sent


def test_extended_can_polling_defaults_on_when_key_missing():
    config = server_config()
    del config["extendedCanPolling"]
    _, sent = run_session(config)
    assert b"$RCAN,185" in sent


# ---------------------------------------------------------------------------
# Single-port mode
# ---------------------------------------------------------------------------

@pytest.fixture
def router():
    services = {imei: SilenceServerService(server_config(), imei) for imei in (IMEI_A, IMEI_B)}
    for service in services.values():
        service.start()     # no own listener: returns at once
    port = free_port()
    router = SinglePortRouter(port, services, login_timeout=2)
    router.start()
    deadline = time.time() + 3
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    yield SimpleNamespace(port=port, services=services)
    router.stop()
    for service in services.values():
        service.stop()


def test_single_port_routes_login_and_commands_by_imei(router):
    vehicle_b = socket.create_connection(("127.0.0.1", router.port))
    vehicle_b.sendall(login_frame(IMEI_B))
    assert read_for(vehicle_b, 0.5) == b"\x06"

    # A command for B reaches B's connection; one for A does not
    pub.sendMessage(imei_topic(TOPIC_COMMAND_RECEIVED, IMEI_A), command="FLASH", payload="")
    pub.sendMessage(imei_topic(TOPIC_COMMAND_RECEIVED, IMEI_B), command="BEEP_FLASH", payload="")
    sent = read_for(vehicle_b, 1.0)
    assert b"$FIND,0,35,1,30,0\r\n" in sent         # BEEP_FLASH
    assert b"$FIND,0,19,1,30,1\r\n" not in sent     # FLASH was for A
    assert router.services[IMEI_B].connectionCount == 1
    assert router.services[IMEI_A].connectionCount == 0
    vehicle_b.close()


def test_single_port_unknown_imei_is_closed(router):
    vehicle = socket.create_connection(("127.0.0.1", router.port))
    vehicle.sendall(login_frame("860000000000099"))
    vehicle.settimeout(3)
    assert vehicle.recv(16) == b""      # closed, no ACK
    vehicle.close()


def test_single_port_garbage_is_closed(router):
    vehicle = socket.create_connection(("127.0.0.1", router.port))
    vehicle.sendall(b"GET / HTTP/1.1\r\n\r\n")
    vehicle.settimeout(3)
    assert vehicle.recv(16) == b""
    vehicle.close()


def test_mask_imei():
    assert mask_imei("860000000000099") == "***********0099"
