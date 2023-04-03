%%%-------------------------------------------------------------------
%% @doc dandelion top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(dandelion_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    ControlTopic = <<"dandelion_commands">>,
    GroupId      = <<"dandelion_group_id">>,
    ClientId     = dandelion_brod_client,
    TrafficServerSpec = #{
        id => dandelion_server,          % mandatory
        start => {dandelion_server, start, [ClientId, GroupId, ControlTopic]},    % mandatory, {m,f,a}
        restart => permanent,            % optional, defaults to permanent
        shutdown => 1000,                % optional, defaults to 5000 (w) or infinity (sup)
        type => worker,                  % optional, defaults to worker
        modules => [dandelion_server]    % optional
    },
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [TrafficServerSpec],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
