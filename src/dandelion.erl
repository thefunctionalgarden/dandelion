-module(dandelion).
-behaviour(brod_group_subscriber).

-include_lib("_build/default/lib/brod/include/brod_int.hrl").
-include_lib("_build/default/lib/brod/include/brod.hrl").
-include_lib("_build/default/lib/kafka_protocol/include/kpro_public.hrl").
-include("../include/log.hrl").


-record(state, { group_id :: binary()
               , offset_dir :: file:fd()
               , message_type  :: message | message_set
               , handlers = [] :: [{{brod:topic(), brod:partition()}, pid()}]
               }).



%% ====================================================================
%% API functions
%% ====================================================================
-export([
         start/7,
         start_traffic/5,
         stop_traffic/0,
         pause_traffic/0,
         process_messages/3,
         set_conf/2
        ]).

-export([
        init/2,
        handle_message/4
        ]).


start(Client, GroupId, Topics, GroupConfig, ConsumerConfig, CbModule, CbInitArg) ->
    brod_group_subscriber:start_link(Client, GroupId, Topics, GroupConfig,
           ConsumerConfig, CbModule, CbInitArg).

init(GroupId, MessageType) ->
    init_conf(),
    {ok, #state{
        group_id = GroupId,
        message_type = MessageType
        }
    }.


start_traffic(SourceTopic, TargetTopic, _ApplicationGroupId, _NumberOfMessages, ProduceUniformMPS) ->
    ?log_info("~p:~p - starting traffic from API", [?MODULE, ?LINE]),

    set_conf(control, ok),

    % fetcher_min_bytes: "10240",
    % fetcher_max_bytes: "102400",
    % fetcher_timeout:   "500",
    
    %% start and pause the data fetchers
    %% get the current offset
    %% set the offset to Highwatermark - NumberOfMessages 

    %% dynamic params
    set_conf(target_topic, TargetTopic),
    set_conf({produce_uniform_mps, TargetTopic}, ProduceUniformMPS),
    
    %% initialize the topic producer
    brod:start_producer(dandelion_brod_client, TargetTopic, [{max_retries, 5}]),

    %% initialize the topic fetcher
    ?log_info("~p:~p - about to start fetching from topic: ~p", [?MODULE, ?LINE, SourceTopic]),

    ok.


handle_message(_SourceTopic, Partition, #kafka_message{} = Message, State) ->
    ok = process_messages(dandelion_brod_client, Partition, Message),
    {ok, ack, State};
handle_message(_SourceTopic, Partition, #kafka_message_set{} = MessageSet,
               #state{ message_type = message_set} = State
    ) ->
    % #kafka_message_set{messages = Messages} = MessageSet,
    process_messages(dandelion_brod_client, Partition, MessageSet),
    {ok, ack, State}.


stop_traffic() ->
    set_conf(control, stop),
    ok.


pause_traffic() ->
    set_conf(control, pause),
    ok.

    

%% ====================================================================
%% Internal functions
%% ====================================================================


init_conf() ->
    TableName = sim_conf_table,
    case ets:info(TableName) of
        undefined ->
            ets:new(TableName, [set, named_table, public]);
        _->
            ok
    end.

get_conf(ConfName) ->
    [{_ConfKey, ConfValue}] = ets:lookup(sim_conf_table, ConfName),
    ConfValue.

set_conf(ConfName, ConfValue) ->
    init_conf(),
    ets:insert(sim_conf_table, {ConfName, ConfValue}).


%% -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -


process_messages(ClientId, Partition, FetchedMessages) ->
    
%%     #{
%%       number_of_messages := NumberOfMessages, 
%%       target_topic := TargetTopic,
%%       high_watemark := HighWatermark
%%      } = Params,
    
    ?log_info("~p:~p - PartitionNum:~p", [?MODULE, ?LINE, Partition]),
    
    Control = get_conf(control),
    Resp = case Control of
        ok ->
            produce_uniform(ClientId, Partition, FetchedMessages),
            ok;
        pause -> pause;
        stop -> stop;
        _Other -> stop 
    end,
    Resp.


produce_uniform(ClientId, Partition, FetchedMessages) ->
    Control = get_conf(control),
    TargetTopic = get_conf(target_topic),
    ProduceUniformMPS = get_conf({produce_uniform_mps, TargetTopic}),
    FetchedMessagesNum = length(FetchedMessages),
    ?log_info("~p:~p - processing...  uniform rate: ~p.  target topic: ~p.  partition: ~p.  batch size: ~p.", 
              [?MODULE, ?LINE, ProduceUniformMPS, TargetTopic, Partition, FetchedMessagesNum]),

    %% divide the fetched element list in 10 lists almost equal in size, and produce each list
    %% this is to produce at a smoother rate 
    FetchedBatchSize = FetchedMessagesNum div 10,
    lists:foldl(
        fun(BatchSize, FetchedMessagesToProcess) ->
            {_FetchedMessagesHeaders, FetchedMessagesRemaining} = lists:split(BatchSize, FetchedMessagesToProcess),
            case Control of
                ok ->
                    % Batch = [{K1, V1}, {K2, V2}, {<<>>, [{K3, V3}]}],
                    % brod:produce_no_ack(ClientId, TargetTopic, Partition, <<>>, Batch),
                    brod:produce_no_ack(ClientId, TargetTopic, Partition, <<>>, FetchedMessagesToProcess),
                    timer:sleep(BatchSize * 1000 div ProduceUniformMPS);
                _Other ->
                    %% Control not ok, skip production
                    true
            end,
            FetchedMessagesRemaining
        end,
        FetchedMessages,
        [FetchedBatchSize, FetchedBatchSize, FetchedBatchSize,
         FetchedBatchSize, FetchedBatchSize, FetchedBatchSize,
         FetchedBatchSize, FetchedBatchSize, FetchedBatchSize,
         FetchedBatchSize + (FetchedMessagesNum rem 10)]
    ),
    
%%     %% produce the batch as it arrives.  too rustic
%%     %% or one by one.  too inefficient
    ok.



