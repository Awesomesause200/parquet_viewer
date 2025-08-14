import os
import json


class ConfigManager:
    """
    ConfigManager is a class that manages a local configuration file so the user can officially "save" their changes.

    The ConfigManager includes a default configuration which includes a version number. This version number helps the
    script understand changes between versions and allows us to create a "patch" that updates configurations without
    outright resetting from nothing.
    """
    def __init__(self):
        self.configuration_location = os.path.join(os.getcwd(), "static", "configuration.json")
        os.makedirs(os.path.dirname(self.configuration_location), exist_ok=True)

        self.default_configuration: dict = {
            "configuration_version": "1.0",
            "null_ratio_alert_threshold": {
                "name": "Null Ratio Alert Threshold",
                "type": "range",
                "value": 0.05,
                "min": 0,
                "max": 1,
                "step": 0.01,
                "description": "Show a warning if converting a column's datatype creates more than this percentage of "
                               "null values."
            }
        }
        self.configuration = self.read_configuration()

    def read_configuration(self) -> dict:
        """
        If the configuration file exists when launching, it will read it. Otherwise, it will create a new configuration
        file based off the default from the class

        :return: dictionary based off either the current configuration or a new one
        """
        if os.path.exists(self.configuration_location):
            with open(self.configuration_location, 'r') as config_fp:
                self.configuration = json.load(config_fp)
        else:
            self.configuration = self.default_configuration
            self.save_configuration()

        return self.configuration

    def save_configuration(self) -> None:
        """
        Saves the current configuration state to the local file system

        :return: Nothing
        """
        with open(self.configuration_location, 'w') as config_fp:
            json.dump(self.configuration, config_fp, indent=4)

    def reset_configuration(self) -> None:
        """
        Resets the configuration based off the default from the class.

        :return: Nothing
        """
        with open(self.configuration_location, 'w') as config_fp:
            json.dump(self.default_configuration, config_fp, indent=4)
        self.configuration = self.read_configuration()


if __name__ == "__main__":
    conf = ConfigManager()
