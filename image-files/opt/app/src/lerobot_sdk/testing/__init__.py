"""Test fixtures only. No physical IO and no production remote control protocol."""
from lerobot_sdk.testing.fake import FakeRobot, simulation_config

__all__=['FakeRobot','simulation_config']
