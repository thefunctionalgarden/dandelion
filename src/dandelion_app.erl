-module(dandelion_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = application:ensure_all_started(brod),
    ok = brod:start_client([{"localhost", 9092}], dandelion_brod_client),  %TODO to be configured
    dandelion_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
