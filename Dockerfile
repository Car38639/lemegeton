# 使用 conda-forge 提供的 Miniforge 映像檔
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /root/workspace
ENV WORKSPACE=/root/workspace

RUN apt update && apt install -y  \
    curl \ 
    cmake \
    vim \ 
    protobuf-compiler

RUN pip3 install protobuf

# 安裝pip依賴
RUN pip3 install \
    pyyaml \
    pyzmq \
    pyserial

# 清理不必要的套件以減少映像檔大小
RUN apt autoremove -y && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]
CMD ["bash"]

