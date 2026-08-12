"""大型數值資料的傳輸：小的內嵌進訊息，大的搬到訊息體外。

把 6MB 的影像塞進 protobuf 的 `bytes` 欄位，送出端要經過 `tobytes()` →
指派欄位 → `SerializeToString()` 三次數 MB 的配置與複製，收端再一次。改成
「protobuf 只描述 shape/dtype，資料接在同一個 frame 尾端」之後就沒有這些複製。

但這個作法有約 10~15µs 的固定開銷（信封、緩衝配置），payload 太小時反而更慢。
實測交叉點在 **50KB** 左右，因此 :func:`encode` 會依大小自動選擇，使用者不必判斷：

===========  =====================  ==================
payload      protobuf 內含 bytes    搬出訊息體
===========  =====================  ==================
2 KB          4.6 µs                 12.7 µs
32 KB        11.9 µs                 14.6 µs
128 KB       59.2 µs                 10.2 µs
2 MB       1333 µs                   56.2 µs
8 MB       6781 µs                  408 µs
===========  =====================  ==================

線路格式（搬出訊息體時，單一 ZMQ frame，因此 `CONFLATE` 照常運作）::

    [b"LBLB"][uint32 信封長度][BlobEnvelope][blob0][blob1]...

開頭的 magic 讓收端能分辨兩種模式，同一個 topic 可以混送。
"""

import struct
from collections.abc import Mapping
from typing import Any, Dict, Tuple

import numpy as np

from lemegeton.msg.common.blob_pb2 import Blob, BlobEnvelope, BlobSpec

MAGIC = b"LBLB"
_LEN = struct.Struct("<I")
_PREFIX = len(MAGIC) + _LEN.size

#: 超過這個大小才搬出訊息體（實測的交叉點）
THRESHOLD = 50 * 1024

BLOB_TYPE = Blob.DESCRIPTOR.full_name
_fields_cache: Dict[str, Tuple[str, ...]] = {}


def is_array(value) -> bool:
    """是不是 ndarray（或其他有 dtype/shape 的緩衝，例如 torch tensor）"""
    return hasattr(value, "dtype") and hasattr(value, "shape")


def is_blob_payload(payload) -> bool:
    """收到的位元組是不是「資料搬出訊息體」的封包"""
    return bytes(memoryview(payload)[: len(MAGIC)]) == MAGIC


def blob_fields(message_class) -> Tuple[str, ...]:
    """走一次 descriptor 找出所有 Blob 型別的欄位路徑（含巢狀），結果快取。"""
    descriptor = message_class.DESCRIPTOR
    if descriptor.full_name in _fields_cache:
        return _fields_cache[descriptor.full_name]

    def walk(desc, prefix, seen):
        if desc.full_name in seen:
            return []
        seen = seen | {desc.full_name}
        found = []
        for field in desc.fields:
            if field.message_type is None or field.is_repeated:
                continue
            if field.message_type.full_name == BLOB_TYPE:
                found.append(f"{prefix}{field.name}")
            else:
                found += walk(field.message_type, f"{prefix}{field.name}.", seen)
        return found

    _fields_cache[descriptor.full_name] = tuple(walk(descriptor, "", set()))
    return _fields_cache[descriptor.full_name]


def _resolve(message, path: str):
    """把 "rgb" 或 "body.data" 這種路徑走到對應的 Blob 子訊息"""
    target = message
    for name in path.split("."):
        target = getattr(target, name)
    return target


def _fill(slot, array) -> None:
    del slot.shape[:]
    slot.shape.extend(array.shape)
    slot.dtype = array.dtype.name


def encode(message, arrays: Mapping[str, Any], threshold: int = THRESHOLD) -> bytes:
    """把訊息與陣列打包成可送出的位元組。

    :param arrays: ``{Blob 欄位路徑: ndarray}``。小於 ``threshold`` 的直接內嵌，
                   其餘搬到訊息體外。
    :return: bytes（全部內嵌）或 bytearray（有搬出，可安全地 ``send(copy=False)``）
    """
    external = {}
    for path, value in arrays.items():
        array = np.ascontiguousarray(value)
        slot = _resolve(message, path)
        _fill(slot, array)
        if array.nbytes >= threshold:
            slot.ClearField("data")
            external[path] = array
        else:
            slot.data = array.tobytes()

    if not external:
        return message.SerializeToString()

    envelope = BlobEnvelope(type_url=message.DESCRIPTOR.full_name)
    payloads, offset = [], 0
    for path, array in external.items():
        envelope.blobs.append(
            BlobSpec(
                field=path,
                shape=list(array.shape),
                dtype=array.dtype.name,
                offset=offset,
                size=array.nbytes,
            )
        )
        payloads.append(memoryview(array).cast("B"))
        offset += array.nbytes

    envelope.header = message.SerializeToString()
    head = envelope.SerializeToString()

    out = bytearray(_PREFIX + len(head) + offset)
    out[: len(MAGIC)] = MAGIC
    _LEN.pack_into(out, len(MAGIC), len(head))
    out[_PREFIX : _PREFIX + len(head)] = head

    base = _PREFIX + len(head)
    view = memoryview(out)
    for spec, payload in zip(envelope.blobs, payloads):
        view[base + spec.offset : base + spec.offset + spec.size] = payload
    return out


def decode(payload, message_class) -> Tuple[Any, Dict[str, Any]]:
    """還原成 ``(訊息, {欄位路徑: ndarray})``，兩種模式都處理。

    .. warning::
       資料搬出訊息體時，回傳的陣列是**接收緩衝的 view（零複製）**，
       只在該緩衝存活期間有效。要保留請自行 ``.copy()``。
    """
    arrays: Dict[str, Any] = {}

    if is_blob_payload(payload):
        buf = memoryview(payload)
        head_len = _LEN.unpack_from(buf, len(MAGIC))[0]
        envelope = BlobEnvelope()
        envelope.ParseFromString(bytes(buf[_PREFIX : _PREFIX + head_len]))

        expected = message_class.DESCRIPTOR.full_name
        if envelope.type_url != expected:
            raise TypeError(f"型別不符：封包是 {envelope.type_url}，預期 {expected}")

        message = message_class()
        message.ParseFromString(envelope.header)

        base = _PREFIX + head_len
        for spec in envelope.blobs:
            raw = buf[base + spec.offset : base + spec.offset + spec.size]
            arrays[spec.field] = np.frombuffer(raw, dtype=np.dtype(spec.dtype)).reshape(
                tuple(spec.shape)
            )
    else:
        message = message_class()
        message.ParseFromString(payload)

    # 內嵌模式的 Blob 欄位也一併還原，讓收端不必分辨兩種模式
    for path in blob_fields(message_class):
        if path in arrays:
            continue
        slot = _resolve(message, path)
        if slot.data:
            arrays[path] = np.frombuffer(slot.data, dtype=np.dtype(slot.dtype)).reshape(
                tuple(slot.shape)
            )
    return message, arrays
