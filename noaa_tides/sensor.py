"""Support for the NOAA Tides and Currents API."""
from datetime import datetime, timedelta
from datetime import timezone as tz
import logging
import requests
import math
from typing import Optional

import noaa_coops as nc
import voluptuous as vol

from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import (
    ATTR_ATTRIBUTION,
    CONF_NAME,
    CONF_TIME_ZONE,
    CONF_UNIT_SYSTEM,
    DEVICE_CLASS_TEMPERATURE,
    DEVICE_CLASS_TIMESTAMP,
    TEMP_CELSIUS,
    TEMP_FAHRENHEIT
)
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity import Entity
from homeassistant.util.unit_system import METRIC_SYSTEM

_LOGGER = logging.getLogger(__name__)

CONF_STATION_ID = "station_id"
CONF_STATION_TYPE = "type"

DEFAULT_ATTRIBUTION = "Data provided by NOAA"
BUOY_ATTRIBUTION = "Data provided by NDBC"
DEFAULT_NAME = "NOAA Tides"
DEFAULT_TIMEZONE = "lst_ldt"

TIMEZONES = ["gmt", "lst", "lst_ldt"]
UNIT_SYSTEMS = ["english", "metric"]
STATION_TYPES = ["tides", "temp", "buoy"]

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_STATION_ID): cv.string,
        vol.Required(CONF_STATION_TYPE): vol.In(STATION_TYPES),
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Optional(CONF_TIME_ZONE, default=DEFAULT_TIMEZONE): vol.In(TIMEZONES),
        vol.Optional(CONF_UNIT_SYSTEM): vol.In(UNIT_SYSTEMS)
    }
)

ghass = None

async def async_setup_platform(hass, config, async_add_entities, discovery_info=None):
    """Set up the NOAA Tides and Currents sensor."""
    global ghass
    ghass = hass
    station_id = config[CONF_STATION_ID]
    station_type = config[CONF_STATION_TYPE]
    name = config.get(CONF_NAME)
    timezone = config.get(CONF_TIME_ZONE)

    if CONF_UNIT_SYSTEM in config:
        unit_system = config[CONF_UNIT_SYSTEM]
    elif hass.config.units is METRIC_SYSTEM:
        unit_system = UNIT_SYSTEMS[1]
    else:
        unit_system = UNIT_SYSTEMS[0]

    if station_type == "tides":
        noaa_sensor = NOAATidesAndCurrentsSensor(name, station_id, timezone, unit_system)
        await hass.async_add_executor_job(noaa_sensor.noaa_coops_update)
    elif station_type == "temp":
        noaa_sensor = NOAATemperatureSensor(name, station_id, timezone, unit_system)
        await hass.async_add_executor_job(noaa_sensor.noaa_coops_update)
    else:
        noaa_sensor = NOAABuoySensor(name, station_id, timezone, unit_system)
        await hass.async_add_executor_job(noaa_sensor.buoy_query)

    async_add_entities([noaa_sensor], True)

class NOAATidesAndCurrentsSensor(Entity):
    """Representation of a NOAA Tides and Currents sensor."""

    def __init__(self, name, station_id, timezone, unit_system):
        """Initialize the sensor."""
        self._name = name
        self._station_id = station_id
        self._timezone = timezone
        self._unit_system = unit_system
        self._station = None
        self.data = None
        self.attr = None

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    def update_tide_factor_from_attr(self):
        _LOGGER.debug("Updating sine fit for tide factor")
        if self.attr is None:
            return
        if ("last_tide_time" not in self.attr or
            "next_tide_time" not in self.attr or
            "next_tide_type" not in self.attr):
            return
        now = datetime.now()
        most_recent = datetime.strptime(self.attr["last_tide_time"], "%I:%M %p")
        next_tide_time = datetime.strptime(self.attr["next_tide_time"], "%I:%M %p")
        predicted_period = (next_tide_time - most_recent).seconds
        if self.attr["next_tide_type"] == "High":
            self.attr["tide_factor"] = 50 - (50*math.cos((now - most_recent).seconds * math.pi / predicted_period))
        else:
            self.attr["tide_factor"] = 50 + (50*math.cos((now - most_recent).seconds * math.pi / predicted_period))

    @property
    def extra_state_attributes(self):
        _LOGGER.debug("extra_state_attributes queried")
        """Return the state attributes of this device."""
        if self.attr is None:
            self.attr = {ATTR_ATTRIBUTION: DEFAULT_ATTRIBUTION}
        if self.data is None:
            return self.attr

        now = datetime.now()
        tide_text = None
        most_recent = None
        for index, row in self.data.iterrows():
            if most_recent == None or (index <= now and index > most_recent):
                most_recent = index
            elif index > now:
                self.attr["next_tide_time"] = index.strftime("%-I:%M %p")
                self.attr["last_tide_time"] = most_recent.strftime("%-I:%M %p")
                tide_factor = 0
                predicted_period = (index - most_recent).seconds
                if row.hi_lo == "H":
                    self.attr["next_tide_type"] = "High"
                    self.attr["last_tide_type"] = "Low"
                    self.attr["high_tide_level"] = row.predicted_wl
                elif row.hi_lo == "L":
                    self.attr["next_tide_type"] = "Low"
                    self.attr["last_tide_type"] = "High"
                    self.attr["low_tide_level"] = row.predicted_wl
                self.update_tide_factor_from_attr()
                return self.attr
        return self.attr

    @property
    def state(self):
        """Return the state of the device."""
        if self.data is None:
            return None
        now = datetime.now()
        for index, row in self.data.iterrows():
            if index > now:
                if row.hi_lo == "H":
                    next_tide = "High"
                if row.hi_lo == "L":
                    next_tide = "Low"
                tide_time = index.strftime("%-I:%M %p")
                return f"{next_tide} tide at {tide_time}"

    def noaa_coops_update(self):
        _LOGGER.debug("update queried.")

        if self._station is None:
            _LOGGER.debug("No station object exists yet- creating one.")
            try:
                self._station = nc.Station(self._station_id)
            except requests.exceptions.ConnectionError as err:
                _LOGGER.error(f"Couldn't create a NOAA station object. Will retry next update. Error: {err}")
                self._station = None
                return

        begin = datetime.now() - timedelta(hours=24)
        begin_date=begin.strftime("%Y%m%d %H:%M")
        end = begin + timedelta(hours=48)
        end_date = end.strftime("%Y%m%d %H:%M")
        try:
            df_predictions = self._station.get_data(
                begin_date=begin_date,
                end_date=end_date,
                product="predictions",
                datum="MLLW",
                interval="hilo",
                units=self._unit_system,
                time_zone=self._timezone,
            )

            self.data = df_predictions
            _LOGGER.debug(f"Data = {self.data}")
            _LOGGER.debug(
                "Recent Tide data queried with start time set to %s",
                begin_date,
            )
        except ValueError as err:
            _LOGGER.error(f"Check NOAA Tides and Currents: {err.args}")
        except requests.exceptions.ConnectionError as err:
            _LOGGER.error(f"Couldn't connect to NOAA Ties and Currents API: {err}")
        return None

    async def async_update(self):
        """Get the latest data from NOAA Tides and Currents API."""
        if not self.data is None:
            # If there are data for a tide > 3 hours away, don't bother querying the NOAA
            min_ts = datetime.now() + timedelta(hours=3)
            for index, row in self.data.iterrows():
                if index > min_ts:
                    _LOGGER.debug("Data exist with a tide in at most 3 hours, not querying NOAA.")
                    return
        ghass.async_add_executor_job(self.noaa_coops_update)

class NOAATemperatureSensor(NOAATidesAndCurrentsSensor):
    """Representation of a NOAA Temperature sensor."""

    @property
    def extra_state_attributes(self):
        """Return the state attributes of this device."""
        if self.attr is None:
            self.attr = {ATTR_ATTRIBUTION: DEFAULT_ATTRIBUTION}
        if self.data is None:
            return self.attr

        if self.data[0] is not None:
            self.attr["temperature"] = self.data[0].water_temp[0]
            self.attr["temperature_time"] = self.data[0].index[0].strftime("%Y-%m-%dT%H:%M")
        if self.data[1] is not None:
            self.attr["air_temperature"] = self.data[1].air_temp[0]
            self.attr["air_temperature_time"] = self.data[1].index[0].strftime("%Y-%m-%dT%H:%M")
        return self.attr

    @property
    def state(self):
        """Return the state of the device."""
        if self.data is None:
            return None
        if self.data[0] is None:
            # If there is no water temperature use the air temperature
            return self.data[1].air_temp[0]
        return self.data[0].water_temp[0]

    @property
    def device_class(self) -> Optional[str]:
        return DEVICE_CLASS_TEMPERATURE

    @property
    def unit_of_measurement(self):
        return TEMP_CELSIUS if self._unit_system == "metric" else TEMP_FAHRENHEIT

    def noaa_coops_update(self):
        if self._station is None:
            _LOGGER.debug("No station object exists yet- creating one.")
            try:
                self._station = nc.Station(self._station_id)
            except requests.exceptions.ConnectionError as err:
                _LOGGER.error(f"Couldn't create a NOAA station object. Will retry next update. Error: {err}")
                self._station = None
                return

        stn = self._station
        end = datetime.now()
        delta = timedelta(minutes=60)
        begin = end - delta
        temps = None
        air_temps = None
        try:
            temps = stn.get_data(
                begin_date=begin.strftime("%Y%m%d %H:%M"),
                end_date=end.strftime("%Y%m%d %H:%M"),
                product="water_temperature",
                units=self._unit_system,
                time_zone=self._timezone,
            ).tail(1)
            _LOGGER.debug(
                "Recent water temperature data queried with start time set to %s",
                begin.strftime("%m-%d-%Y %H:%M"),
            )
        except ValueError as err:
            _LOGGER.error(f"Check NOAA Tides and Currents: {err.args}")
        except requests.exceptions.ConnectionError as err:
            _LOGGER.error(f"Couldn't connect to NOAA Ties and Currents API: {err}")

        try:
            air_temps = stn.get_data(
                begin_date=begin.strftime("%Y%m%d %H:%M"),
                end_date=end.strftime("%Y%m%d %H:%M"),
                product="air_temperature",
                units=self._unit_system,
                time_zone=self._timezone,
            ).tail(1)
            _LOGGER.debug(
                "Recent temperature data queried with start time set to %s",
                begin.strftime("%m-%d-%Y %H:%M"),
            )
        except ValueError as err:
            _LOGGER.error(f"Check NOAA Tides and Currents: {err.args}")
        except requests.exceptions.ConnectionError as err:
            _LOGGER.error(f"Couldn't connect to NOAA Ties and Currents API: {err}")
        if temps is None and air_temps is None:
            self.data = None
        else:
            self.data = (temps, air_temps)
        _LOGGER.debug(f"Data = {self.data}")

    async def async_update(self):
        """Get the latest data from NOAA Tides and Currents API."""
        ghass.async_add_executor_job(self.noaa_coops_update)


class NOAABuoySensor(Entity):
    """Representation of a NOAA Buoy."""
    FMT_URI="https://www.ndbc.noaa.gov/data/realtime2/%s.txt"

    def __init__(self, name, station_id, timezone, unit_system):
        """Initialize the sensor."""
        self._name = name
        self._station_url = self.FMT_URI % station_id
        self._timezone = timezone
        self._unit_system = unit_system
        self.data = None
        self.attr = None

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def device_class(self) -> Optional[str]:
        return DEVICE_CLASS_TEMPERATURE

    @property
    def unit_of_measurement(self):
        return TEMP_CELSIUS if self._unit_system == "metric" else TEMP_FAHRENHEIT

    @property
    def extra_state_attributes(self):
        """Return the state attributes of this device."""
        if self.attr is None:
            self.attr = {ATTR_ATTRIBUTION: BUOY_ATTRIBUTION}
        if self.data is None:
            return self.attr

        data_time = datetime(self.data["YY"][1], self.data["MM"][1], self.data["DD"][1],
                hour=self.data["hh"][1], minute=self.data["mm"][1], tzinfo=tz.utc)
        for k in self.data:
            if k in ("YY", "MM", "DD", "hh", "mm"):
                continue
            if self.data[k][1] == "MM":
                # continue here lets us retain the old values when there are no data availabile
                continue

            if self._timezone == "gmt":
                self.attr[k + "_time"] = data_time.strftime("%Y-%m-%dT%H:%M")
            else:
                self.attr[k + "_time"] = data_time.replace(tzinfo=tz.utc).astimezone(tz=None).strftime("%Y-%m-%dT%H:%M")

            if self._unit_system == "english" and self.data[k][0] == "degC":
                self.attr[k + "_unit"] = "degF"
                self.attr[k] = round((self.data[k][1] * 9 / 5) + 32, 1)
            else:
                self.attr[k + "_unit"] = self.data[k][0]
                self.attr[k] = self.data[k][1]

        return self.attr

    @property
    def state(self):
        """Return the state of the device."""
        if self.data is None:
            return None
        if self.data["WTMP"] is None:
            return None
        if self.data["WTMP"][1] == "MM":
            return None
        if self._unit_system == "metric":
            return self.data["WTMP"][1]
        return round((self.data["WTMP"][1] * 9 / 5) + 32, 1)

    def buoy_query(self):
        _LOGGER.debug("Querying the buoy database")
        r = requests.get(self._station_url)
        if r.status_code is not requests.codes.ok:
            _LOGGER.error(f"Received HTTP code {r.status_code} from {self._station_url} query")
            return
        # r.text is new-line separated with #-prefixed headers for data type and unit.
        # since temperature is always celsius, if unit_system is english, convert.

        lines = r.text.splitlines()
        if len(lines) < 3:
            _LOGGER.error(f"Received fewer than 3 lines of data, which shouldn't happen: {r.text}")

        if self.data == None:
            self.data = {}
        head = '\n    '.join(lines[0:3])
        _LOGGER.debug(f"Buoy data head:\n    {head}")
        fields = lines[0].strip("#").split()
        units = lines[1].strip("#").split()
        values = lines[2].split() # latest values are at the top of the file, thankfully.
        for i in range(len(fields)):
            if values[i] == "MM":
                self.data[fields[i]] = (units[i], values[i])
            elif "." in values[i]:
                self.data[fields[i]] = (units[i], float(values[i]))
            else:
                self.data[fields[i]] = (units[i], int(values[i]))

    async def async_update(self):
        """Get the latest data from NOAA Buoy API."""
        ghass.async_add_executor_job(self.buoy_query)
