"""TCU reply text for MQTT: what the car answered to a command.

Every text frame the vehicle sends after login ($FIND,OK, $IPAD,<host>, TEST:...)
except its $ASTRA login/heartbeat. $STMS carries the SIM ICCID, MSISDN and VIN
at indices 7, 8 and 9 (docs/COMMANDS.md in the outer repo): blanked here so they
never leave this process.

Named-command text arguments ($MESG) are validated with TEXT_ARG_RE.
"""
import re

REPLY_MAX = 512
STMS_PRIVATE = (7, 8, 9)
PRIVATE = "[private]"

# Dashboard message text: letters, digits, spaces and basic punctuation, 1-40
# characters. No '$' (starts a TCU command), no ',' (field separator), no CR/LF.
TEXT_ARG_RE = re.compile(r"^[A-Za-z0-9 .!?'()/:&+-]{1,40}$")


def valid_text_arg(text):
    return isinstance(text, str) and bool(TEXT_ARG_RE.match(text)) and text.strip() == text and text != ""


def reply_text(data):
    """Reply text for one vehicle frame (bytes), or None if it isn't a reply."""
    text = bytes(data).decode(errors="replace").strip("\r\n\x00 ")
    if not text or text.startswith("$ASTRA"):
        return None
    if text.startswith("$STMS,"):
        fields = text.split(",")
        for i in STMS_PRIVATE:
            if i < len(fields):
                fields[i] = PRIVATE
        text = ",".join(fields)
    return text[:REPLY_MAX]
