-define(
    log_info(LogMessage),
    logger:log(info, LogMessage)
).
-define(
    log_warning(LogMessage),
    logger:log(warning, LogMessage)
).
-define(
    log_error(LogMessage),
    logger:log(error, LogMessage)
).
-define(
    log_debug(LogMessage),
    logger:log(debug, LogMessage)
).

-define(
    log_info(LogMessage, LogParameters),
    logger:log(info, io_lib:format(LogMessage, LogParameters))
).
-define(
    log_warning(LogMessage, LogParameters),
    logger:log(warning, io_lib:format(LogMessage, LogParameters))
).
-define(
    log_error(LogMessage, LogParameters),
    logger:log(error, io_lib:format(LogMessage, LogParameters))
).
-define(
    log_debug(LogMessage, LogParameters),
    logger:log(debug, io_lib:format(LogMessage, LogParameters))
).