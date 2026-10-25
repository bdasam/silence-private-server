"""S04 named commands for the admin console, the $MESG text argument and the
TCU reply topic. Loopback sockets only; bridgeMode off; fake IMEIs."""
import json
import socket
import threading
from types import SimpleNamespace

import pytest
from pubsub import pub

from helpers.constants import *
from helpers.reply import reply_text, valid_text_arg
from services.CommandService import CommandService
from services.MQTTService import MQTTService
from services.SilenceServerService import SilenceServerService
from tests.test_s04 import IMEI_A, MQTT_CONFIG, login_frame, read_for, server_config

with open("helpers/commands_definition.json") as f:
    DEFINITIONS = json.load(f)

S04 = {
    "S04_STATUS": "$STMS", "S04_TEST": "$TEST", "S04_IMEI": "$IMEI",
    "S04_SERVER_ADDRESS": "$IPAD", "S04_SERVER_PORT": "$PORT", "S04_APN": "$APAD",
    "S04_BLEC": "$BLEC", "S04_CPWR": "$CPWR", "S04_REPL": "$REPL",
    "S04_BATTERY_REPORT": "$RCAN,184",
    "S04_TRANSPORT_MODE": "$SCAN,31B,0,8,24,0,0,0,0,0,0,0$RCAN,31C",
    "S04_LOCATE": "$FIND,0,19,1,5,1", "S04_POLL": "$POLL", "S04_LOGIN": "$LOGN",
    "S04_MESSAGE": "$MESG,{text}, 5",
    "S04_POWER_ON": "$PUNL,1$DIAG,1,50$SDIG,2,1", "S04_POWER_OFF": "$SDIG,2,0",
    "S04_REBOOT": "$BOOT",
}


def test_s04_definitions_are_exact():
    got = {k: v["command"] for k, v in DEFINITIONS.items() if k.startswith("S04_")}
    assert got == {k: v + "\r\n" for k, v in S04.items()}
    for name in S04:
        assert DEFINITIONS[name]["maxretry"] == 1       # never resent on its own


def test_s01_definitions_unchanged():
    assert DEFINITIONS["FLASH"]["command"] == "$FIND,0,19,1,30,1\r\n"
    assert DEFINITIONS["TURN_ON_SCOOTER"]["command"] == "$PWON,4\r\n"
    assert DEFINITIONS["STOP_FLASH"]["command"] == "$PWON,0\r\n"


def test_no_forbidden_strings_in_definitions():
    for name, entry in DEFINITIONS.items():
        cmd = entry["command"]
        for bad in ("$FACT", "$LOAD", "$DBUG", "$DIAG,3", "$SCAN,31A", "$SCAN,233", "$IPAD1", "$PORT1", "$APAD,"):
            assert bad not in cmd, name
        if "$SCAN" in cmd:
            assert name == "S04_TRANSPORT_MODE"


@pytest.mark.parametrize("text,ok", [
    ("Return by 5pm", True), ("Hi! Call 441-555-0100.", True), ("x" * 40, True),
    ("x" * 41, False), ("", False), ("a$STMS", False), ("a,b", False), ("a\r\n$BOOT", False),
    ("a\nb", False), (" lead", False), ("café", False), ("semi;colon", False),
])
def test_text_arg_validation(text, ok):
    assert valid_text_arg(text) is ok


@pytest.fixture
def published(monkeypatch):
    import services.CommandService as cs_mod
    calls = []
    monkeypatch.setattr(cs_mod.pub, "sendMessage", lambda *a, **kw: calls.append(kw))
    return calls


def service_with_definitions():
    svc = CommandService({}, IMEI_A)
    svc.commands_definition = dict(DEFINITIONS)
    return svc


def test_message_text_is_put_in_place(published):
    svc = service_with_definitions()
    svc.command_received("S04_MESSAGE", "Return by 5pm")
    cmd = svc.command_queue.get()
    assert cmd.get_command() == bytearray(b"$MESG,Return by 5pm, 5\r\n")
    # the definition itself still holds the placeholder
    assert svc.commands_definition["S04_MESSAGE"]["command"] == "$MESG,{text}, 5\r\n"


@pytest.mark.parametrize("bad", ["", "a,b", "x$BOOT", "a\r\n$FACT", "y" * 41])
def test_message_with_bad_text_is_not_queued(published, bad):
    svc = service_with_definitions()
    svc.command_received("S04_MESSAGE", bad)
    assert svc.command_queue.qsize() == 0
    assert published[-1]["result"] == "Invalid text"


def test_payload_is_ignored_for_commands_without_text(published):
    svc = service_with_definitions()
    svc.command_received("S04_POWER_OFF", "$FACT")
    assert svc.command_queue.get().get_command() == bytearray(b"$SDIG,2,0\r\n")


def test_reply_text():
    assert reply_text(b"$FIND,OK\r\n") == "$FIND,OK"
    assert reply_text(b"TEST:HW 1.2,FW 7.0\r\n") == "TEST:HW 1.2,FW 7.0"
    assert reply_text(b"$ASTRA;AT402;860000000000011;;7.0.61.35;Z;0\r\n") is None
    assert reply_text(b"\r\n") is None
    assert len(reply_text(b"$X," + b"9" * 2000)) == 512


def test_stms_private_fields_are_blanked():
    fields = ["$STMS", "3", "1", "0", "0", "0", "22", "8934071234567890123", "14415550100", "VINXYZ123"] + ["5"] * 30
    out = reply_text((",".join(fields) + "\r\n").encode()).split(",")
    assert out[7:10] == ["[private]"] * 3
    assert out[1] == "3" and len(out) == len(fields)


def test_mqtt_publishes_reply_on_reply_topic():
    svc = MQTTService(MQTT_CONFIG, IMEI_A)
    sent = []
    svc.client = SimpleNamespace(publish=lambda topic, value, retain=False: sent.append((topic, value, retain)))
    svc.command_reply("$FIND,OK")
    assert sent == [(f"home/silence-server/{IMEI_A}/reply", "$FIND,OK", False)]


def test_vehicle_replies_reach_the_reply_topic():
    replies = []

    def listener(reply):
        replies.append(reply)

    pub.subscribe(listener, imei_topic(TOPIC_COMMAND_REPLY, IMEI_A))
    service = SilenceServerService(server_config(), IMEI_A)
    vehicle, server_side = socket.socketpair()
    thread = threading.Thread(target=service.handle_client, args=(server_side,), daemon=True)
    thread.start()
    try:
        vehicle.sendall(login_frame(IMEI_A))
        assert read_for(vehicle, 0.5) == b"\x06"
        vehicle.sendall(b"$FIND,OK\r\n")
        read_for(vehicle, 0.5)
        vehicle.sendall(b"TEST:HW 1.2\r\n")      # no leading $: used to close the connection
        read_for(vehicle, 0.5)
        vehicle.sendall(b"$POLL,OK\r\n")
        read_for(vehicle, 0.5)
        assert replies == ["$FIND,OK", "TEST:HW 1.2", "$POLL,OK"]
        assert service.connectionCount == 1     # still connected
    finally:
        vehicle.close()
        thread.join(3)
        service.stop()
        pub.unsubscribe(listener, imei_topic(TOPIC_COMMAND_REPLY, IMEI_A))
