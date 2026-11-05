class SDKError(Exception):
    """Base exception for local SDK operations."""

class ConfigurationError(SDKError, ValueError): pass
class ContractMismatch(SDKError, ValueError): pass
class DeviceDisconnected(SDKError): pass
class BusyError(SDKError): pass
class DataNotReady(SDKError): pass
class OperationFailed(SDKError): pass
class OperationCancelled(SDKError): pass
class WaitTimeout(SDKError, TimeoutError): pass
class StateError(SDKError): pass
