"""Best-effort operational logging without changing observed application work."""


def emit(logger, level, message, *args):
    """Call standard logging with fixed messages and safe control fields only.

    Application-owned handlers, filters and formatters can fail. Their ordinary
    exceptions must not become capture failures or replace a native outcome.
    """
    try:
        logger.log(level, message, *args)
    except Exception:
        pass
