import json
from decimal import Decimal
from uuid import UUID

from dramatiq import Encoder, DecodeError
from dramatiq.encoder import MessageData


class ExtendJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class JSONEncoder(Encoder):
    """Encodes messages as JSON
    """

    def encode(self, data: MessageData) -> bytes:
        return json.dumps(data, cls=ExtendJSONEncoder, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> MessageData:
        try:
            data_str = data.decode("utf-8")
        except UnicodeDecodeError as e:
            raise DecodeError("failed to decode data %r" % (data,), data, e) from None

        try:
            return json.loads(data_str)
        except json.decoder.JSONDecodeError as e:
            raise DecodeError("failed to decode message %r" % (data_str,), data_str, e) from None
