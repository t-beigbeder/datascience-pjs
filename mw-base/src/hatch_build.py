import pathlib
import os

from hatchling.metadata.plugin.interface import MetadataHookInterface


class MetaDataHook(MetadataHookInterface):
    def update(self, metadata):
        v = os.getenv("V_P11MWB_V")
        if not v:
            raise ValueError("V_P11MWB_V environment variable shoud be set before building this package")
        metadata["version"] = v
        metadata["dependencies"] = []
        with pathlib.Path(self.root, "requirements.txt").open() as if_:
            for ln in if_:
                ln = ln[:-1]
                if ln.startswith("# "):
                    continue
                if not ln.startswith("../") and "file:///" not in ln:
                    metadata["dependencies"].append(ln)
                    continue
                if "file:///" in ln:
                    # will be installed by dockerfile explicit pip install
                    continue
                # ../ddpbasics => ddpbasics @ file:///path/to/ddpbasics
                metadata["dependencies"].append(f"{ln[3:]} @ file://{str(pathlib.Path(ln).resolve())}")
