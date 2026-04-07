"""Process exit codes matching the Go and .NET implementations."""


class ExitCodes:
    SUCCESS = 0
    FATAL_ERROR = 1
    INTENTIONAL_RESTART = 4  # session renewal, HQ-initiated restart, upgrade
    NATS_DISCONNECTED = 5
