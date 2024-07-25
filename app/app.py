"""
Any device over TCP/IP connections. Reads, parses, and publishes data.

@ToDo
2. Change sonic wind data sensor name.
"""

import socket
import logging
from waggle.plugin import Plugin
from collections import OrderedDict
import re
import argparse
import timeout_decorator
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TIMEOUT_SECONDS = 300




def connect(args):
    """
    Connect to a device.

    :param args: input argument object
    :return: A socket object for communication.
    """
    try:
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.connect((args.ip, args.port))
    except Exception as e:
        logging.error(f"Connection failed: {e}. Check device or network.")
        raise
    return tcp_socket


def publish_data(plugin, data, data_names, meta):
    """
    Publishes data to the beehive.

    :param plugin: Plugin object for publishing.
    :param data: Data dictionary to be published.
    :param data_names: Mapping of data keys to publishing names.
    :param meta: Metadata for the data.
    """
    if data:
        timestamp_nanoseconds = int(data.get('Seconds', 0) * 1e9) # 0 if not found

        for key, value in data.items():
            if key in data_names:
                try:
                    meta_data = {
                        "units": meta["units"][data_names[key]],
                        "description": meta["description"][data_names[key]],
                        "name": data_names[key],
                        "sensor": meta["sensor"],
                    }
                    plugin.publish(data_names[key], value, meta=meta_data, 
                                timestamp=timestamp_nanoseconds)
                except KeyError as e:
                    logging.error(f"Metadata key missing: {e}")



@timeout_decorator.timeout(TIMEOUT_SECONDS, use_signals=True)
def parse_data(args, tcp_socket):
    try:
        data = tcp_socket.recv(4096).decode("utf-8")
    except Exception as e:
        logging.error(f"Error getting data: {e}")
        raise

    return extract_data(data)


def extract_data(data):
    parsed_data = {}
    # Ratterns and keys for specific device
    patterns = {
        'U': r'\(U ([-\d.]+)\)',
        'V': r'\(V ([-\d.]+)\)',
        'W': r'\(W ([-\d.]+)\)',
        'TS': r'\(TS ([-\d.]+)\)',
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, data)
        if match:
            try:
                parsed_data[key] = float(match.group(1))
            except ValueError:
                parsed_data[key] = match.group(1)
    
    return parsed_data



def run(args, data_names, meta):
    with Plugin() as plugin:
        try:
            tcp_socket = connect(args)
            while True:
                data = parse_data(args, tcp_socket)
                # logging.info(f"Data: {data}")
                publish_data(plugin, data, data_names, meta)
        except timeout_decorator.TimeoutError:
            logging.error(f"Unknown_Timeout")
            plugin.publish('exit.status', 'Unknown_Timeout')
            sys.exit("Timeout error while waiting for data.")
        except Exception as e:
            logging.error(f"{e}")
        finally:
            if tcp_socket:
                tcp_socket.close()
            logging.info("Connection closed.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Data Interface for Any Device")
    parser.add_argument('--ip', type=str, required=True, help='Device IP address')
    parser.add_argument('--port', type=int, default=7200, help='TCP connection port (default: 5000)')
    parser.add_argument('--sensor', type=str, required=True, help='Sensor names')
    parser.add_argument('--timeout', type=int, default=300, help='Timeout interval in seconds (default: 300)')

    args = parser.parse_args()

    # get timeout in seconds
    os.environ['TIMEOUT_SECONDS'] = str(args.timeout)

    # data_names and meta
    data_names = OrderedDict([
        ("U", "sonic.u"),
        ("V", "sonic.v"),
        ("W", "sonic.w"),
        ("TS", "sonic.temperature"),
    ])

    meta = {
        "sensor": args.sensor,
        "units": {
            "sonic.u": "m/s",
            "sonic.v": "m/s",
            "sonic.w": "m/s",
            "sonic.temperature": "°C",
        },
        "description": {
            "sonic.u": "Sonic U-component of wind speed",
            "sonic.v": "Sonic V-component of wind speed",
            "sonic.w": "Sonic vertical wind speed",
            "sonic.temperature": "Sonic temperature",
        },
    }

    try:
        run(args, data_names, meta)
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down.")
    except Exception as e:
        logging.error(f"Startup failed: {e}")
    finally:
        logging.info("Application terminated.")

