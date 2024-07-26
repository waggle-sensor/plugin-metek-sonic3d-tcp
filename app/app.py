"""
Any device over TCP/IP connections. Reads, parses, and publishes data.

@ToDo
2. Change sonic wind data sensor name.
"""

import socket
import logging
from waggle.plugin import Plugin, get_timestamp
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
        
        response = tcp_socket.recv(4096).decode("utf-8")
        tcp_socket.sendall(f"{args.username}\r\n".encode())
        response = tcp_socket.recv(4096).decode("utf-8")
        tcp_socket.sendall(f"{args.password}\r\n".encode())

        # Handle the failed authentication 
        response = tcp_socket.recv(4096).decode("utf-8")
        if "authentication successful" not in response.lower():
            raise Exception("Authentication failed.")
        
    except Exception as e:
        logging.error(f"Connection failed: {e}. Check device or network.")
        raise
    return tcp_socket





def publish_data(plugin, data, data_names, meta, additional_meta=None):
    """
    Publishes data to the plugin.

    :param plugin: Plugin object for publishing data.
    :param data: Dictionary of data to be published.
    :param data_names: Mapping of data keys to their publishing names.
    :param meta: Metadata associated with the data.
    :param additional_meta: Additional metadata to be included.
    """

    if not data:
        logging.warning("No data to publish.")
        plugin.publish("status", "NoData", meta={"timestamp": get_timestamp()})
        return

    for key, value in data.items():
        if key in data_names:
            try:
                meta_data = {
                    "missing": "-9999.0",
                    "units": meta["units"][data_names[key]],
                    "description": meta["description"][data_names[key]],
                    "name": data_names[key],
                    "sensor": meta["sensor"],
                }
                if additional_meta:
                    meta_data.update(additional_meta)

                timestamp = get_timestamp()
                plugin.publish(
                    data_names[key], value, meta=meta_data, timestamp=timestamp
                )
            except KeyError as e:
                plugin.publish('status', f'{e}')
                print(f"Error: Missing key in meta data - {e}")








@timeout_decorator.timeout(TIMEOUT_SECONDS, use_signals=True)
def parse_data(args, tcp_socket, data_names):
    try:
        line = tcp_socket.recv(4096).decode("utf-8").rstrip().split(";")[1:5]
    except Exception as e:
        logging.error(f"Error getting data: {e}")
        raise


    if not line or len(line) < len(data_names):
        logging.info(line)
        logging.warning("Empty or incomplete data line received.")
        pass #raise ValueError("Empty or incomplete data line.")

    keys = data_names.keys()
    values = [float(value) for value in line]
    data_dict = dict(zip(keys, values))
    return data_dict




def run(args, data_names, meta):
    with Plugin() as plugin:
        tcp_socket = None
        try:
            tcp_socket = connect(args)
            while True:
                data = parse_data(args, tcp_socket, data_names)
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
    parser.add_argument('--username', type=str, default="data", help='Username for TCP connection')
    parser.add_argument('--password', type=str, default="METEKGMBH", help='Password for TCP connection')
    parser.add_argument('--sensor', type=str, required=True, help='Sensor names')
    parser.add_argument('--timeout', type=int, default=300, help='Timeout interval in seconds (default: 300)')

    args = parser.parse_args()

    # get timeout in seconds
    os.environ['TIMEOUT_SECONDS'] = str(args.timeout)

# data_names and meta
data_names = OrderedDict([
    ("U", "sonic3d.uwind"),
    ("V", "sonic3d.vwind"),
    ("W", "sonic3d.wwind"),
    ("T", "sonic3d.temp"),
])

meta = {
    "sensor": args.sensor,
     "units": {
            "sonic3d.uwind": "m/s",
            "sonic3d.vwind": "m/s",
            "sonic3d.wwind": "m/s",
            "sonic3d.temp": "degrees Celsius",
        },
        "description": {
            "sonic3d.uwind": "zonal wind",
            "sonic3d.vwind": "meridional wind",
            "sonic3d.wwind": "vertical wind",
            "sonic3d.temp": "Ambient Temperature",
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

