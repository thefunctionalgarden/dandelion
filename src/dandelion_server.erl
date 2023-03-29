-module(dandelion_server).
-behaviour(gen_server).

-include("../include/log.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

-export([
         start/0,
         set_params/1,
         process_control_messages/5
        ]).


start() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

set_params(ParamsMap) ->
    gen_server:call(?MODULE, {set_params, ParamsMap}).

start_traffic(ApplicationGroupId) ->
    gen_server:call(?MODULE, {start_traffic, [ApplicationGroupId]}).

stop_traffic(ApplicationGroupId) ->
    gen_server:call(?MODULE, {stop_traffic, [ApplicationGroupId]}).

pause_traffic(ApplicationGroupId) ->
    gen_server:call(?MODULE, {pause_traffic, [ApplicationGroupId]}).



%% ====================================================================
%% Behavioural functions
%% ====================================================================

init([]) ->
    ?log_info("~p:~p - starting traffic server", [?MODULE, ?LINE]),

    ControlTopicMinB  = 40,
    ControlTopicMaxB  = 1024,
    ControlTopicTO    = 50,
    ControlTopic      = <<"dandelion_commands">>,
    
    GroupId        = <<"dandelion">>,
    GroupPostfix   = integer_to_binary(-rand:uniform(100000000)),
    CommandsGroupId = <<GroupId/bitstring, GroupPostfix/bitstring>>,

    % ?log_info("~p:~p     - ControlTopic: ~p", [?MODULE, ?LINE, ControlTopic]),
    % ?log_info("~p:~p     - ConfGroupId:  ~p", [?MODULE, ?LINE, CommandsGroupId]),

    %% initialize the control topic fetcher
    FetcherFunctionParams = [{list_to_atom(binary_to_list(ControlTopic)),
                                    {?MODULE, process_control_messages, []}}],
    %% start and pause the data fetchers
    dandelion_fetcher:start(
        ControlTopic, CommandsGroupId, "true.", FetcherFunctionParams,
        latest, ok, ControlTopicMinB, ControlTopicMaxB, ControlTopicTO
    ),
    {ok, #{state => <<"stopped">>, group_id => GroupId}}.


%% setting the sim parameters
handle_call({set_params, SimParamsMap}, _From, State) ->
    
    %TODO search the correct sim, by equalizing ApplicationGroupId and GroupId

    #{
      <<"source_topic">> := SourceTopicStr,
      <<"target_topic">> := TargetTopicStr,
      <<"application_group_id">> := ApplicationGroupId,
      <<"produce_uniform_mps_global">> := ProduceUniformMPSGlobalStr,
      <<"number_of_messages">> := NumberOfMessages
      } = SimParamsMap,

    % SourceTopic = string:lexemes(SourceTopicStr, ", "),
    
    %% aprox number of Messages per second for each partition
    NumOfPartitions = brod:get_partitions_count(dandelion_brod_client, SourceTopicStr),
    ProduceUniformMPS = (binary_to_integer(ProduceUniformMPSGlobalStr) div NumOfPartitions) + 1,


    ?log_info("~p:~p     - SourceTopic:             ~p", [?MODULE, ?LINE, SourceTopicStr]),
    ?log_info("~p:~p     - TargetTopic:             ~p", [?MODULE, ?LINE, TargetTopicStr]),
    ?log_info("~p:~p     - ApplicationGroupId:      ~p", [?MODULE, ?LINE, ApplicationGroupId]),
    ?log_info("~p:~p     - Partitions:              ~p", [?MODULE, ?LINE, NumOfPartitions]),
    ?log_info("~p:~p     - ProduceUniformMPSGlobal: ~p", [?MODULE, ?LINE, ProduceUniformMPSGlobalStr]),
    ?log_info("~p:~p     - ProduceUniformMPS /part: ~w", [?MODULE, ?LINE, ProduceUniformMPS]),
    ?log_info("~p:~p     - NumberOfMessages:        ~p", [?MODULE, ?LINE, NumberOfMessages]),

    State2 = maps:merge(State, SimParamsMap),
    NewState = maps:put(<<"produce_uniform_mps">>, ProduceUniformMPS, State2),
    
    %%   (((  dynamic params setting here  )))   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    dandelion:set_conf({produce_uniform_mps, TargetTopicStr}, ProduceUniformMPS),
    
    {reply, ok, NewState};



%% starting the sim
handle_call({start_traffic, [_ApplicationGroupId]}, _From, State) ->
    ?log_info("~p:~p - start_traffic instance", [?MODULE, ?LINE]),
    #{
      <<"source_topic">> := SourceTopic,
      <<"target_topic">> := TargetTopic,
      <<"application_group_id">> := GroupId,
      <<"number_of_messages">> := NumberOfMessagesBS,
      <<"produce_uniform_mps">> := ProduceUniformMPS
      } = State,
    
    ?log_info("~p:~p     - SourceTopic:      ~p", [?MODULE, ?LINE, SourceTopic]),
    ?log_info("~p:~p     - TargetTopic:      ~p", [?MODULE, ?LINE, TargetTopic]),
    ?log_info("~p:~p     - GroupId:          ~p", [?MODULE, ?LINE, GroupId]),
    ?log_info("~p:~p     - NumberOfMessages: ~p", [?MODULE, ?LINE, NumberOfMessagesBS]),

    %TODO search the correct sim, by equalizing ApplicationGroupId and GroupId

    dandelion:start_traffic(SourceTopic, TargetTopic, GroupId, binary_to_integer(NumberOfMessagesBS), ProduceUniformMPS),
    NewState = maps:put(state, <<"started">>, State),
    {reply, ok, NewState};

%% stopping the sim
handle_call({stop_traffic, [_ApplicationGroupId]}, _From, State) ->
    
    %TODO search the correct sim, by equalizing ApplicationGroupId and GroupId

    ?log_info("~p:~p - stop_traffic", [?MODULE, ?LINE]),
    dandelion:stop_traffic(),
    NewState = maps:put(state, <<"stopped">>, State),
    {reply, ok, NewState};

%% pausing the sim
handle_call({pause_traffic, [_ApplicationGroupId]}, _From, State) ->
    
    %TODO search the correct sim, by equalizing ApplicationGroupId and GroupId

    ?log_info("~p:~p - pause_traffic", [?MODULE, ?LINE]),
    dandelion:pause_traffic(),
    NewState = maps:put(state, <<"paused">>, State),
    {reply, ok, NewState}.



handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.





%% ====================================================================
%% Internal functions
%% ====================================================================


process_control_messages(_Cluster, _Group, _SourceTopic, _PartitionNum, 
                 {[], FetchedElements}
                ) ->
    %% process each message
    lists:foreach(fun
                     (#{key := K, value := V}) ->
                          ?log_info("~p:~p - fetched key: ~p", [?MODULE, ?LINE, K]),
                          ?log_info("~p:~p - fetched value: ~p", [?MODULE, ?LINE, V]),
                          SimControl = jsx:decode(V, [return_maps]),
                          process_control_message(SimControl);
                     (UnexpectedMessage) ->
                          ?log_warning("~p:~p - Ignoring message from control topic: ~p",
                                     [?MODULE, ?LINE, UnexpectedMessage])
                  end, FetchedElements),
    ok.


process_control_message(
  #{
    <<"application_group_id">> := ApplicationGroupId, 
    <<"command">>              := <<"start">> 
    }
) ->
    start_traffic(ApplicationGroupId);

process_control_message(
  #{
    <<"application_group_id">> := ApplicationGroupId, 
    <<"command">>              := <<"stop">> 
    }
) ->
    stop_traffic(ApplicationGroupId);

process_control_message(
  #{
    <<"application_group_id">> := ApplicationGroupId, 
    <<"command">>              := <<"pause">> 
    }
) ->
    pause_traffic(ApplicationGroupId);

process_control_message(
  #{
    <<"params">> := ParamsMap
   }
) ->
    set_params(ParamsMap);

process_control_message(UnexpectedMessage) ->
    ?log_error("~p:~p - Unexpected Message from control topic: ~p", [?MODULE, ?LINE, UnexpectedMessage]),
    error.
    

%% -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -



