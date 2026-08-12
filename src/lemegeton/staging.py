"""事後指派的語法糖。

protobuf 的 repeated 與子訊息欄位不允許事後指派::

    msg.shape = [1080, 1920, 3]     # AttributeError
    msg.pelvis = Pose()             # AttributeError

只能用建構子（`Image(shape=[...])`）或 `extend()` / `CopyFrom()`。本模組提供一層
「組裝用的鷹架」，讓事後指派可行，送出時再一次轉成真正的 protobuf 訊息::

    msg = lemegeton.build(TeleopFrame)
    msg.header.frame_id = 7          # 巢狀欄位自動建立
    msg.body.joints = ["pelvis", "waist"]
    publisher.send(msg)              # send() 會自動呼叫 to_proto()

設計取捨（皆有實測）：

* **鷹架只用於組裝，讀取一律走 protobuf 本體。** 若改用攔截 `__setattr__` 的
  proxy，讀取會付 2.5 倍開銷 —— 對 1000Hz 的熱路徑不划算。
* **`to_dict()` + 單次建構子**，而不是由下往上逐層建 protobuf 物件。前者讓 C 層
  一次建完整棵樹，實測快 1.9 倍。
* **只走「有設過」的欄位。** 掃過全部欄位再判斷會慢 2.3 倍（訊息通常是稀疏填值）。
* **類別由 descriptor 執行期生成並快取**，不做 codegen —— 否則 `.proto` 加欄位時
  會靜默漂移。

成本（`TeleopData`，5 個 Pose + 50 個 dexterous，相對原生建構子）：只在外層用
約 1.4 倍、全程巢狀約 4.4 倍。1000Hz 下分別是 1.5% 與 4.8% 的單核佔用。熱路徑
若在意，直接用 protobuf 建構子即可 —— 兩種寫法可以混用。
"""

from typing import Any, Dict, Tuple

_cache: Dict[str, type] = {}


class Staging:
    """組裝中的訊息。欄位語意與對應的 protobuf 訊息完全相同。"""

    __slots__ = ("_values",)

    _pb = None  # 對應的 protobuf 類別
    _fields: Dict[str, Any] = {}  # name -> FieldDescriptor
    _msg_fields: frozenset = frozenset()  # 子訊息欄位（非 repeated）

    def __init__(self, **kwargs):
        object.__setattr__(self, "_values", {})
        for key, value in kwargs.items():
            setattr(self, key, value)

    # -- 指派 ---------------------------------------------------------------
    def __setattr__(self, name: str, value):
        field = self._fields.get(name)
        if field is None:
            raise AttributeError(
                f"{self._pb.DESCRIPTOR.full_name} 沒有欄位 '{name}'"
                f"（可用欄位：{', '.join(sorted(self._fields))}）"
            )
        if field.is_repeated and isinstance(value, (str, bytes)):
            raise TypeError(
                f"欄位 '{name}' 是 repeated，需要序列而不是 {type(value).__name__}"
            )
        self._values[name] = value

    # -- 讀取 ---------------------------------------------------------------
    def __getattr__(self, name: str):
        values = object.__getattribute__(self, "_values")
        if name in values:
            return values[name]

        cls = type(self)
        field = cls._fields.get(name)
        if field is None:
            raise AttributeError(f"{cls._pb.DESCRIPTOR.full_name} 沒有欄位 '{name}'")
        if name in cls._msg_fields:
            # 子訊息：比照 protobuf 的行為，一存取就自動建立，
            # 讓 msg.header.frame_id = 7 這種寫法可行
            nested = staging_for(_pb_class(field.message_type))()
            values[name] = nested
            return nested
        if field.is_repeated:
            fresh = []
            values[name] = fresh
            return fresh
        raise AttributeError(
            f"欄位 '{name}' 尚未設定"
            f"（{cls._pb.DESCRIPTOR.full_name} 的純量欄位需先指派才能讀取）"
        )

    def __contains__(self, name: str) -> bool:
        return name in self._values

    def __repr__(self) -> str:
        body = ", ".join(f"{k}={v!r}" for k, v in self._values.items())
        return f"{self._pb.DESCRIPTOR.name}({body})"

    # -- 轉換 ---------------------------------------------------------------
    def _split(self, prefix: str = "") -> Tuple[dict, Dict[str, Any]]:
        """遞迴拆成 (純 dict, {欄位路徑: ndarray})。

        ndarray 不會進 dict —— 它們會交給 :mod:`lemegeton.blob` 決定要內嵌
        還是搬到訊息體外，避免無謂地經過 protobuf 的 bytes 欄位。
        """
        from lemegeton import blob

        out: dict = {}
        arrays: Dict[str, Any] = {}
        for name, value in self._values.items():
            path = f"{prefix}{name}"
            if blob.is_array(value):
                arrays[path] = value
            elif isinstance(value, Staging):
                nested, nested_arrays = value._split(f"{path}.")
                arrays.update(nested_arrays)
                if nested or nested_arrays:  # 空的自動建立子訊息不需要送
                    out[name] = nested
            elif isinstance(value, (list, tuple)) and name in self._msg_fields:
                out[name] = [
                    v.to_dict() if isinstance(v, Staging) else v for v in value
                ]
            elif isinstance(value, (list, tuple)) and not value:
                continue
            else:
                out[name] = value
        return out, arrays

    def to_dict(self) -> dict:
        """遞迴轉成純 dict，交給 protobuf 建構子一次建完整棵樹。"""
        return self._split()[0]

    def to_proto(self):
        """組裝成真正的 protobuf 訊息（不含尚未編碼的陣列）。"""
        return self._pb(**self.to_dict())

    def build_payload(self) -> Tuple[Any, Dict[str, Any]]:
        """組裝成 ``(protobuf 訊息, {Blob 欄位路徑: ndarray})``，供傳輸層使用。"""
        fields, arrays = self._split()
        return self._pb(**fields), arrays


_pb_registry: Dict[str, type] = {}


def _register(message_class) -> None:
    _pb_registry[message_class.DESCRIPTOR.full_name] = message_class


def _pb_class(descriptor):
    """由 descriptor 取回對應的 protobuf 類別"""
    full_name = descriptor.full_name
    if full_name not in _pb_registry:
        # 產生的 _pb2 模組會把類別掛在 descriptor 的 _concrete_class 上
        concrete = getattr(descriptor, "_concrete_class", None)
        if concrete is None:
            from google.protobuf import message_factory

            concrete = message_factory.GetMessageClass(descriptor)
        _pb_registry[full_name] = concrete
    return _pb_registry[full_name]


def staging_for(message_class) -> type:
    """為某個 protobuf 訊息類別生成（並快取）對應的鷹架類別。"""
    descriptor = message_class.DESCRIPTOR
    full_name = descriptor.full_name
    if full_name in _cache:
        return _cache[full_name]

    _register(message_class)
    cls = type(
        f"Staging_{descriptor.name}",
        (Staging,),
        {
            "__slots__": (),
            "_pb": message_class,
            "_fields": {f.name: f for f in descriptor.fields},
            "_msg_fields": frozenset(
                f.name for f in descriptor.fields if f.message_type is not None
            ),
        },
    )
    _cache[full_name] = cls
    return cls


def build(message_class, **kwargs) -> Staging:
    """建立一個可事後指派的訊息鷹架。

    ``lemegeton.build(TeleopFrame, header=...)`` 也可以直接帶初值。
    """
    return staging_for(message_class)(**kwargs)


def to_message(value):
    """鷹架轉 protobuf；其他型別原樣回傳。供 Requester / ActionClient 內部使用。"""
    return value.to_proto() if isinstance(value, Staging) else value


def to_payload(value) -> Tuple[Any, Dict[str, Any]]:
    """鷹架轉 ``(protobuf 訊息, 陣列)``；其他型別回傳 ``(原值, {})``。"""
    if isinstance(value, Staging):
        return value.build_payload()
    return value, {}
