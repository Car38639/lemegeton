from multiprocessing import shared_memory
from typing import Optional

import numpy as np


class ShmHost:
    def __init__(self, data_shape=(1024,), data_type=np.uint8):
        """ShmHost handles shared memory segments for inter-process communication.

        Use double buffering with two shared memory segments (shm_0 and shm_1) and a control segment (shm_ctrl) to manage synchronization.
        """
        self.data_shape = data_shape
        data_size = np.prod(data_shape) * np.dtype(data_type).itemsize
        self.data_type = data_type

        self.shm0 = shared_memory.SharedMemory(create=True, size=data_size)
        self.shm0_buf = np.ndarray(
            data_shape, dtype=self.data_type, buffer=self.shm0.buf
        )

        self.shm1 = shared_memory.SharedMemory(create=True, size=data_size)
        self.shm1_buf = np.ndarray(
            data_shape, dtype=self.data_type, buffer=self.shm1.buf
        )

        self.shm2 = shared_memory.SharedMemory(create=True, size=data_size)
        self.shm2_buf = np.ndarray(
            data_shape, dtype=self.data_type, buffer=self.shm2.buf
        )

        self.shm_ctrl = shared_memory.SharedMemory(create=True, size=1)
        self.shm_ctrl.buf[0] = 0

    def set_data(self, data):
        # Todo: id check for 3 buffers

        # 判斷背後緩衝區 (Back Buffer)
        current_idx = self.shm_ctrl.buf[0]
        next_idx = current_idx + 1 if current_idx < 2 else 0

        # target_shm_array = self.shm0_buf if next_idx == 0 else self.shm1_buf if next_idx == 1 else self.shm2_buf
        if next_idx == 0:
            target_shm_array = self.shm0_buf
        elif next_idx == 1:
            target_shm_array = self.shm1_buf
        elif next_idx == 2:
            target_shm_array = self.shm2_buf
        else:
            raise ValueError(f"Invalid buffer index: {next_idx}")

        # 寫入資料
        np.copyto(target_shm_array, data)

        # 原子切換：更新控制信號
        self.shm_ctrl.buf[0] = next_idx

    def get_metadata(self):
        return {
            "ctrl": self.shm_ctrl.name,
            "shm_0": self.shm0.name,
            "shm_1": self.shm1.name,
            "shm_2": self.shm2.name,
            "data_shape": self.data_shape,
            "data_type": self.data_type.__name__,
        }

    def release(self):
        self.shm0.close()
        self.shm0.unlink()

        self.shm1.close()
        self.shm1.unlink()

        self.shm2.close()
        self.shm2.unlink()

        self.shm_ctrl.close()
        self.shm_ctrl.unlink()


class ShmReader:
    def __init__(self, shm_metadata, is_consumer: bool = True):
        """ShmReader connects to existing shared memory segments for inter-process communication.

        Args:
            shm_metadata (dict): A dictionary containing the metadata of the shared memory segments.
        """
        self.shm0, self.shm1, self.shm2, self.shm_ctrl = None, None, None, None
        try:
            self.data_shape = shm_metadata["data_shape"]
            self.data_type = shm_metadata["data_type"]

            data_size = (
                np.prod(self.data_shape) * np.dtype(shm_metadata["data_type"]).itemsize
            )

            self.data_type = np.dtype(shm_metadata["data_type"])

            self.shm_ctrl = shared_memory.SharedMemory(
                name=shm_metadata["ctrl"], size=1
            )

            self.shm0 = shared_memory.SharedMemory(
                name=shm_metadata["shm_0"],
                size=data_size,
            )
            self.shm1 = shared_memory.SharedMemory(
                name=shm_metadata["shm_1"],
                size=data_size,
            )
            self.shm2 = shared_memory.SharedMemory(
                name=shm_metadata["shm_2"],
                size=data_size,
            )
            self.data_buffer = np.empty(np.prod(self.data_shape), dtype=self.data_type)
            self.data_buffer_reshape = self.data_buffer.reshape(self.data_shape)

            if is_consumer:
                from multiprocessing import resource_tracker

                resource_tracker.unregister(self.shm_ctrl._name, "shared_memory")
                resource_tracker.unregister(self.shm0._name, "shared_memory")
                resource_tracker.unregister(self.shm1._name, "shared_memory")
                resource_tracker.unregister(self.shm2._name, "shared_memory")

        except FileNotFoundError as e:
            print(f"Error connecting to shared memory: {e}")

    @property
    def shape(self):
        return self.data_shape

    @property
    def dtype(self):
        return self.data_type

    def get_data(self) -> Optional[np.array]:
        try:
            idx = self.shm_ctrl.buf[0]
            if idx == 0:
                target_shm = self.shm0
            elif idx == 1:
                target_shm = self.shm1
            elif idx == 2:
                target_shm = self.shm2
            else:
                print(f"Invalid buffer index: {idx}")
                return None

            data = np.frombuffer(target_shm.buf, dtype=self.data_type)
            np.copyto(self.data_buffer, data)

            return self.data_buffer_reshape
        except FileNotFoundError as e:
            print(f"{e}")
            return None

        except AttributeError as e:
            print(f"{e}")
            return None

    def release(self):
        if self.shm0:
            self.shm0.close()
        if self.shm1:
            self.shm1.close()
        if self.shm2:
            self.shm2.close()
        if self.shm_ctrl:
            self.shm_ctrl.close()
