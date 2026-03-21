#!/usr/bin/env python3
"""Remove a contact from the MeshCore device by pubkey prefix."""
import asyncio
import configparser
import sys
from meshcore import MeshCore

TARGET = "1cf2676176e9"  # blk2

async def main():
    config = configparser.ConfigParser()
    config.read("config.ini")
    mc_port = config.get("MeshCore", "serial_port", fallback="/dev/ttyUSB0")
    print(f"Connecting to {mc_port}...")
    mc = await MeshCore.create_serial(mc_port, 115200)
    if not mc:
        print("Failed to connect")
        sys.exit(1)

    await mc.commands.send_appstart()
    contacts = await mc.commands.get_contacts(lastmod=0)

    if contacts and contacts.payload:
        for pk, contact in contacts.payload.items():
            prefix = pk[:12]
            name = contact.get("adv_name", "")
            if prefix == TARGET:
                print(f"Found: {name} (pk={pk[:16]}...)")
                result = await mc.commands.remove_contact(contact)
                rtype = getattr(result, 'type', None)
                print(f"Remove result: {rtype}")
                break
        else:
            print(f"Contact {TARGET} not found on device")
    else:
        print("No contacts returned")

    await mc.disconnect()
    print("Done")

asyncio.run(main())
