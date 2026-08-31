#!/usr/bin/env python3
# pylint: disable=too-few-public-methods,invalid-name,too-many-instance-attributes

"""
A Wi-Fi device presence detector for Home Assistant that runs on OpenWRT
"""

import argparse
import json
import queue
import signal
import subprocess
import syslog
import time
from dataclasses import dataclass
from enum import IntEnum
from queue import Queue
from threading import Thread
from typing import Any, Callable

VERSION = "3.2.0"


class Logger:
    """Class to handle logging to syslog"""

    def __init__(self, enable_debug: bool) -> None:
        self.enable_debug = enable_debug

    def log(self, text: str, is_debug: bool = False) -> None:
        """Log a line to syslog. Only log debug messages when debugging is enabled."""
        if is_debug and not self.enable_debug:
            return

        level = syslog.LOG_DEBUG if is_debug else syslog.LOG_INFO
        syslog.openlog(
            ident="presence-detector",
            facility=syslog.LOG_DAEMON,
            logoption=syslog.LOG_PID,
        )
        syslog.syslog(level, text)


class Settings:
    """Loads all settings from a JSON file and provides built-in defaults"""

    def __init__(self, config_file: str) -> None:
        self._settings = {
            "mode": "rest",
            "publishers": {
                "rest": {
                    "url": "http://homeassistant.local:8123",
                    "token": ""
                },
                "mqtt": {
                    "host": "192.168.1.50",
                    "port": 1883,
                    "username": "ha",
                    "password": "",
                    "retain_state": True
                }
            },
            "interfaces": [],
            "filter_is_denylist": True,
            "filter": [],
            "params": {},
            "location": "home",
            "away": "not_home",
            "fallback_sync_interval": 0,
            "source_type": "router",
            "debug": False,
        }
        with open(config_file, "r", encoding="utf-8") as settings:
            user_settings = json.load(settings)
        self._settings = Settings.deep_merge(self._settings, user_settings)

        # Lowercase all MAC addresses in the filter and params settings
        self._settings["filter"] = [device.lower() for device in self.filter]
        self._settings["params"] = {
            device.lower(): params for device, params in self.params.items()
        }
        if not self._settings["interfaces"]:
            self._settings["interfaces"] = self.list_wifi_interfaces()

    def __getattr__(self, item: str) -> Any:
        return self._settings.get(item)

    def list_wifi_interfaces(self) -> list[str]:
        """List all wifi interfaces"""
        output = subprocess.run(
            ["ubus", "list", "hostapd.*"], stdout=subprocess.PIPE, check=True
        )
        return output.stdout.decode("utf-8").strip().split("\n")

    @staticmethod
    def deep_merge(dict1: dict, dict2: dict):
        """Deep merge two dictionaries"""
        result = dict1.copy()
        for key, value in dict2.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = Settings.deep_merge(result[key], value)
            else:
                result[key] = value
        return result


@dataclass
class QueueItem:
    """Represents a device item on the queue"""

    class Action(IntEnum):
        """Possible queue item actions"""

        ADD = 1
        DELETE = 2
        QUIT = 3

    device: str
    interface: str
    action: Action

class Publisher:
    """Interface for a presence-state transport (REST, MQTT, etc.)"""

    def ha_seen(self, device: str, seen: bool = True) -> bool:
        raise NotImplementedError

    def update_version_entity(self) -> None:
        pass

    def on_full_sync(self) -> None:
        pass

    def stop(self) -> None:
        pass
    
class RESTPublisher(Publisher):
    """Publishes presence state to Home Assistant via the REST states API"""

    _request = None
    
    def __init__(self, settings: Settings, logger: Logger) -> None:
        from urllib import request
        RESTPublisher._request = request

        self._settings = settings
        self._logger = logger

        config = settings.publishers["rest"]
        self._url = config["url"]
        self._token = config["token"]
        
        if not self._url or not self._token:
            raise RuntimeError("REST mode requires 'publishers.rest.url' and 'publishers.rest.token' to be set")
    
    @classmethod
    def _post(cls, url: str, data: dict, headers: dict) -> tuple[str, bool]:
        req = cls._request.Request(
            url, data=json.dumps(data).encode("utf-8"), headers=headers
        )
        with cls._request.urlopen(req, timeout=5) as response:
            return response.read(), response.code < 400

    def ha_seen(self, device: str, seen: bool = True) -> bool:
        if seen:
            location = self._settings.location
        else:
            location = self._settings.away

        object_id = device.lower().replace(":", "_")
        attributes = {"source_type": self._settings.source_type, "mac": device}

        if device in self._settings.params:
            attributes.update(self._settings.params[device])

        if self._settings.ap_name:
            attributes["ap_name"] = self._settings.ap_name
            object_id = f"{self._settings.ap_name}_{object_id}"

        entity_id = f"device_tracker.{object_id}"
        body = {"state": location, "attributes": attributes}

        self._logger.log(f"Posting to HA: {body}", True)

        try:
            response, ok = self._post(
                f"{self._url}/api/states/{entity_id}",
                data=body,
                headers={"Authorization": f"Bearer {self._token}"},
            )
            self._logger.log(f"API Response: {response!r}", is_debug=True)
        except Exception as ex:  # pylint: disable=broad-except
            self._logger.log(str(ex), is_debug=True)
            return False
        return ok

    def update_version_entity(self) -> None:
        ap_name = (
            self._settings.ap_name.replace("-", "_").lower()
            if self._settings.ap_name else "openwrt_router"
        )
        entity_id = f"sensor.{ap_name}_presence_detector_version"
        try:
            response, ok = self._post(
                f"{self._url}/api/states/{entity_id}",
                data={"state": VERSION},
                headers={"Authorization": f"Bearer {self._token}"},
            )
        except Exception as ex:  # pylint: disable=broad-except
            ok = False
            response = str(ex)
        if not ok:
            self._logger.log(f"Unable to create/update version entity in HA: {response}")

class MQTTPublisher(Publisher):
    """Publishes presence state to Home Assistant via MQTT Discovery"""

    _mqtt_module = None
    
    def __init__(self, settings: Settings, logger: Logger, on_ha_online) -> None:
        from paho.mqtt import client as mqtt
        MQTTPublisher._mqtt_module = mqtt

        self._settings = settings
        self._logger = logger


        config = settings.publishers["mqtt"]
        self._host = config["host"]
        self._port = config["port"]
        self._username = config["username"]
        self._password = config["password"]
        self._retain_state = config["retain_state"]
        
        self._on_ha_online = on_ha_online   # callback into the detector's _do_full_sync
        self._registered_clients: set[str] = set()
        self._connect_to_mqtt()

    def _connect_to_mqtt(self):
        mqtt = MQTTPublisher._mqtt_module
        if hasattr(mqtt, "CallbackAPIVersion"):
            self._mqtt = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2
            )
        else:
            # Version 1 is deprecated but still supported
            self._mqtt = mqtt.Client()
        self._mqtt.on_connect = self._on_mqtt_connect
        self._mqtt.on_disconnect = self._on_mqtt_disconnect
        if hasattr(self._mqtt, "on_connect_fail"):
            self._mqtt.on_connect_fail = self._on_mqtt_connect_fail
        self._mqtt.username_pw_set(
            self._username, self._password
        )
        self._mqtt.reconnect_delay_set(min_delay=1, max_delay=60)
        self._mqtt.message_callback_add(
            "homeassistant/status", self._on_ha_status_message
        )
        self._mqtt.connect_async(
            self._host, self._port, keepalive=60
        )
        self._mqtt.loop_start()

    def _on_mqtt_connect(
        self, _client, _userdata, _flags, reason_code, _properties=None
    ):
        """Callback for MQTT connection (supports both v1 and v2 API)"""
        is_failure = (
            reason_code.is_failure
            if hasattr(reason_code, "is_failure")
            else reason_code != 0
        )
        if is_failure:
            self._logger.log(f"MQTT broker connection failed (rc: {reason_code})")
            return
        self._logger.log("MQTT broker connected")
        self._mqtt.subscribe("homeassistant/status")

    def _on_mqtt_connect_fail(self, _client, _userdata):
        """Callback for MQTT connection failures"""
        self._logger.log("MQTT broker connection failed, retrying...")

    def _on_mqtt_disconnect(self, *args, **_kwargs):
        """Callback for MQTT disconnections (supports both v1 and v2 API)"""
        reason_code = args[3] if len(args) >= 4 else (args[2] if len(args) >= 3 else 0)
        self._logger.log(f"MQTT broker disconnected (rc: {reason_code})")
        self._registered_clients.clear()

    def _on_ha_status_message(self, _client, _userdata, message):
        """Callback for HA status messages"""
        if message.payload == b"offline":
            self._logger.log("Home Assistant is offline!")
            self._registered_clients.clear()
        elif message.payload == b"online":
            self._logger.log("Home Assistant is back online")
            self._on_ha_online()

    def _publish(self, topic: str, data: str, retain=False) -> bool:
        self._logger.log(f"Publishing to {topic}: {data}", True)
        if not self._mqtt.is_connected():
            return False
        result = self._mqtt.publish(topic, data, qos=1, retain=retain)
        try:
            result.wait_for_publish(timeout=5)
        except (RuntimeError, ValueError) as ex:
            self._logger.log(f"Error publishing to {topic}: {ex}", False)
            return False
        return result.is_published()

    def ha_seen(self, device: str, seen: bool = True) -> bool:
        """Publish MQTT messages register the device and update home/away status"""
        device_slug = device_name = device.replace(":", "_")
        if self._settings.ap_name:
            device_slug = f"{self._settings.ap_name}_{device_slug}"

        ok = True
        if device_slug not in self._registered_clients:
            self._registered_clients.add(device_slug)
            body = {
                "state_topic": f"homeassistant/device_tracker/{device_slug}/state",
                "json_attributes_topic": f"homeassistant/device_tracker/{device_slug}/state",
                "value_template": "{{ value_json['state'] }}",
                "name": device_name,
                "platform": "device_tracker",
                "payload_home": self._settings.location,
                "payload_not_home": self._settings.away,
                "source_type": self._settings.source_type,
                "device": {"connections": [["mac", device]]},
                "unique_id": device_slug,
            }
            if device in self._settings.params:
                body = Settings.deep_merge(body, self._settings.params[device])
                if "name" not in body["device"]:
                    body["device"]["name"] = body["name"]
            # Register the device in HA
            ok &= self._publish(
                f"homeassistant/device_tracker/{device_slug}/config", json.dumps(body)
            )
        # Set the state of the device
        state = {
            "in_zones": [f"zone.{self._settings.location}"] if seen else [],
            "state": self._settings.location if seen else self._settings.away,
        }
        ok &= self._publish(
            f"homeassistant/device_tracker/{device_slug}/state",
            json.dumps(state),
            retain=self._retain_state,
        )
        return ok

    def on_full_sync(self) -> None:
        self._registered_clients = set()

    def stop(self) -> None:
        self._mqtt.disconnect()
        self._mqtt.loop_stop()

class PresenceDetector(Thread):
    def __init__(self, config_file: str) -> None:
        super().__init__()
        self._settings = Settings(config_file)
        self._logger = Logger(self._settings.debug)
        self._queue: Queue = Queue()
        self._watchers: list[UbusWatcher] = []
        self._killed = False
        self._last_seen_clients: set[tuple[str, str]] = set()
        self._online_clients: dict[str, set[str]] = {}
        for interface in self._settings.interfaces:
            self._online_clients[interface] = set()
        self._publisher: Publisher = self._build_publisher()

    def _build_publisher(self) -> Publisher:
        mode = self._settings.mode
        if mode == "rest":
            return RESTPublisher(self._settings, self._logger)
        if mode == "mqtt":
            return MQTTPublisher(self._settings, self._logger, self._do_full_sync)
        raise RuntimeError(f"Unknown or empty mode: {mode!r}")

    def _ha_seen(self, device: str, seen: bool = True) -> bool:
        return self._publisher.ha_seen(device, seen)

    def _update_version_entity(self):
        self._publisher.update_version_entity()

    def set_device_away(self, interface: str, device: str) -> None:
        """Mark a client as away in HA"""
        if not self._should_handle_device(device):
            return
        if device in self._online_clients[interface]:
            self._online_clients[interface].remove(device)
        for intf in set(self._settings.interfaces) - {interface}:
            if device in self._online_clients[intf]:
                # Device is still connected to another interface -> ignore
                self._logger.log(
                    f"Device {device} still connected to {intf}, ignoring away event.",
                    True,
                )
                return
        self._queue.put(QueueItem(device, interface, QueueItem.Action.DELETE))
        self._logger.log(f"Device {device} on {interface} is now away")

    def set_device_home(self, interface: str, device: str) -> None:
        """Add client to the 'add' queue"""
        if not self._should_handle_device(device):
            return
        self._queue.put(QueueItem(device, interface, QueueItem.Action.ADD))
        self._online_clients[interface].add(device)
        self._logger.log(
            f"Device {device} on {interface} is now at {self._settings.location}"
        )

    def _get_all_online_devices(self) -> list[tuple[str, str]]:
        """Call ubus and get all online devices"""
        devices = []
        for interface in self._settings.interfaces:
            process = subprocess.run(
                ["ubus", "call", interface, "get_clients"],
                capture_output=True,
                text=True,
                check=False,
            )
            if process.returncode != 0:
                self._logger.log(
                    f"Error running ubus for interface {interface}: {process.stderr}"
                )
                continue
            response: dict = json.loads(process.stdout)
            devices.extend([(interface, key) for key in response["clients"].keys()])
        return devices

    def _should_handle_device(self, device: str) -> bool:
        """Check if a device should be handled by checking the allow/deny list"""
        if device in self._settings.filter:
            return not self._settings.filter_is_denylist
        return self._settings.filter_is_denylist

    def start_watchers(self) -> None:
        """Start ubus watcher threads for every interface"""
        self._logger.log(
            f"Starting ubus watchers on interfaces {self._settings.interfaces}"
        )
        for interface in self._settings.interfaces:
            # Start an ubus watcher for every interface
            watcher = UbusWatcher(interface, self.set_device_home, self.set_device_away)
            watcher.start()
            self._watchers.append(watcher)

    def stop_watchers(self) -> None:
        """Signal all ubus watchers to stop"""
        for watcher in self._watchers:
            watcher.stop()

    @property
    def stopped(self):
        """Should this Thread be stopped?"""
        return self._killed

    def stop(self, _signum: int | None = None, _frame: int | None = None):
        """Stop this thread as soon as possible"""
        self._logger.log("Stopping...")
        self.stop_watchers()
        self._killed = True
        self._queue.put(QueueItem("quit", "", QueueItem.Action.QUIT))
        self._publisher.stop()

    def _do_full_sync(self, away_only=False):
        """Perform a full sync of all current online devices compared to last time"""
        self._publisher.on_full_sync()
        seen_now = set(self._get_all_online_devices())
        away = self._last_seen_clients - seen_now
        self._last_seen_clients = seen_now
        for interface, client in seen_now:
            if not away_only:
                self.set_device_home(interface, client)
        for interface, client in away:
            self.set_device_away(interface, client)

    def run(self) -> None:
        """Main loop for the presence detector"""
        self._do_full_sync()

        # Update the version entity in HA
        self._update_version_entity()

        # Start ubus watcher(s) for every interface
        self.start_watchers()

        ha_is_offline = False
        # Enable a queue timeout if fallback_sync interval is set
        queue_timeout = (
            self._settings.fallback_sync_interval
            if self._settings.fallback_sync_interval > 0
            else None
        )

        # The main (sync) polling loop
        while not self._killed:
            try:
                item: QueueItem = self._queue.get(timeout=queue_timeout)
            except queue.Empty:
                # Perform a periodic full sync
                self._do_full_sync()
                continue

            if item.action == QueueItem.Action.QUIT:
                self._queue.task_done()
                break

            if self._ha_seen(item.device, item.action == QueueItem.Action.ADD):
                if ha_is_offline:
                    # We're back online -> process backlog
                    ha_is_offline = False
                    self._do_full_sync()
                    # Update the version entity in HA
                    self._update_version_entity()
            else:
                self._logger.log("Home Assistant seems to be offline, sleeping...")
                # HA is offline -> Add the item back to the queue
                # and perform a full sync when it's back
                self._queue.put(item)
                ha_is_offline = True
                time.sleep(5)

            self._queue.task_done()


class UbusWatcher(Thread):
    """Watches live ubus events and signals presence detector of leave/join events"""

    def __init__(
        self,
        interface: str,
        on_join: Callable[[str, str], None],
        on_leave: Callable[[str, str], None],
    ) -> None:
        super().__init__()
        self._on_join = on_join
        self._on_leave = on_leave
        self._interface = interface
        self._killed = False

    def stop(self):
        """Stops this watcher thread"""
        self._killed = True

    def run(self) -> None:
        """Main loop for the ubus event watcher thread"""
        while not self._killed:
            # pylint: disable=consider-using-with
            ubus = subprocess.Popen(
                ["ubus", "subscribe", self._interface],
                stdout=subprocess.PIPE,
                text=True,
            )
            # Give ubus time to start and/or fail
            time.sleep(1)
            # Check if it failed to start
            return_code = ubus.poll()
            if return_code is not None or ubus.stdout is None:
                # Starting ubus failed -> interface does not exist (yet)? let's retry later
                ubus.wait()
                continue
            # Startup OK, start reading stdout
            while not self._killed:
                line = ubus.stdout.readline()
                event = {}
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    # Ignore incomplete / invalid json
                    pass
                if "assoc" in event:
                    self._on_join(self._interface, event["assoc"]["address"].lower())
                elif "disassoc" in event:
                    self._on_leave(
                        self._interface, event["disassoc"]["address"].lower()
                    )
            ubus.terminate()
            ubus.wait()


def main():
    """Main entrypoint: parse arguments and start all threads"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help="Filename of configuration file",
        default="/etc/config/presence-detector.settings.json",
    )
    args = parser.parse_args()

    detector = PresenceDetector(args.config)
    detector.start()
    signal.signal(signal.SIGTERM, detector.stop)
    signal.signal(signal.SIGINT, detector.stop)

    while not detector.stopped:
        time.sleep(1)


if __name__ == "__main__":
    main()
