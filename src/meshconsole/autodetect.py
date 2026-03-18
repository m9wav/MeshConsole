"""
MeshConsole USB Auto-Detection
-------------------------------
Scans serial ports and identifies MeshCore/Meshtastic devices.

Author: M9WAV
License: MIT
"""

import logging
from dataclasses import dataclass
from typing import Optional

import serial.tools.list_ports

from meshconsole.models import BackendType

logger = logging.getLogger(__name__)


@dataclass
class DetectedDevice:
    port: str
    backend_type: BackendType
    device_name: str = ""


def scan_serial_ports() -> list[str]:
    """Return serial port paths that likely have LoRa devices."""
    ports = []
    for port_info in serial.tools.list_ports.comports():
        # Skip Bluetooth ports
        if 'bluetooth' in port_info.description.lower() or 'Bluetooth' in port_info.device:
            continue
        ports.append(port_info.device)
    return sorted(ports)


def probe_meshcore(port: str, timeout: float = 6.0) -> Optional[DetectedDevice]:
    """Try to connect to a port as MeshCore. Returns DetectedDevice or None."""
    try:
        from meshconsole.backend.meshcore import MESHCORE_AVAILABLE
        if not MESHCORE_AVAILABLE:
            return None
    except ImportError:
        return None

    import asyncio
    try:
        from meshcore import MeshCore
    except ImportError:
        return None

    async def _probe():
        mc = await asyncio.wait_for(
            MeshCore.create_serial(port, 115200),
            timeout=timeout
        )
        if mc is None:
            return None
        info = await mc.commands.send_appstart()
        name = ""
        if info and hasattr(info, 'payload') and info.payload:
            name = info.payload.get('name', '') or info.payload.get('adv_name', '')
        await mc.disconnect()
        return DetectedDevice(port=port, backend_type=BackendType.MESHCORE, device_name=name)

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_probe())
    except Exception as e:
        logger.debug(f"MeshCore probe failed on {port}: {e}")
        return None
    finally:
        loop.close()


def probe_meshtastic(port: str, timeout: float = 10.0) -> Optional[DetectedDevice]:
    """Try to connect to a port as Meshtastic. Returns DetectedDevice or None."""
    try:
        from meshconsole.backend.meshtastic import MESHTASTIC_AVAILABLE
        if not MESHTASTIC_AVAILABLE:
            return None
    except ImportError:
        return None

    try:
        import meshtastic.serial_interface
    except ImportError:
        return None

    try:
        iface = meshtastic.serial_interface.SerialInterface(devPath=port)
        name = ""
        if hasattr(iface, 'myInfo') and iface.myInfo:
            name = getattr(iface.myInfo, 'my_node_num', '')
            if name:
                name = f"!{name:08x}"
        iface.close()
        return DetectedDevice(port=port, backend_type=BackendType.MESHTASTIC, device_name=name)
    except Exception as e:
        logger.debug(f"Meshtastic probe failed on {port}: {e}")
        return None


def auto_detect_devices() -> list[DetectedDevice]:
    """Scan serial ports and identify MeshCore/Meshtastic devices."""
    ports = scan_serial_ports()
    if not ports:
        logger.info("No serial ports found")
        return []

    logger.info(f"Scanning {len(ports)} serial port(s): {', '.join(ports)}")
    detected = []
    used_ports = set()

    # Probe MeshCore first (fails fast)
    for port in ports:
        logger.info(f"Probing {port} for MeshCore...")
        result = probe_meshcore(port)
        if result:
            logger.info(f"  Found MeshCore device: {result.device_name or 'unnamed'} on {port}")
            detected.append(result)
            used_ports.add(port)
            break  # Only one MeshCore device for now

    # Probe remaining ports for Meshtastic
    for port in ports:
        if port in used_ports:
            continue
        logger.info(f"Probing {port} for Meshtastic...")
        result = probe_meshtastic(port)
        if result:
            logger.info(f"  Found Meshtastic device: {result.device_name or 'unnamed'} on {port}")
            detected.append(result)
            used_ports.add(port)
            break  # Only one Meshtastic device for now

    if not detected:
        logger.warning("No mesh devices detected on any serial port")

    return detected
