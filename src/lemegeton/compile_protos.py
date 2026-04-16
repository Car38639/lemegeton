# my_package/cli.py
import pathlib
import subprocess


def run_bash():
    script_path = pathlib.Path(__file__).parent / "compile_protos.sh"
    subprocess.run(["bash", str(script_path)])
