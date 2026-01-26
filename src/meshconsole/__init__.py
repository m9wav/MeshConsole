"""
MeshConsole - A tool for interacting with Meshtastic devices.

Author: M9WAV
License: MIT
"""

__version__ = "2.2.0"
__author__ = "M9WAV"

from meshconsole.core import MeshtasticTool, MeshtasticToolError, PacketSummary

__all__ = ["MeshtasticTool", "MeshtasticToolError", "PacketSummary", "__version__"]
