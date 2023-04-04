-module(dandelion_server).
-behaviour(brod_group_subscriber_v2).

-include_lib("brod/include/brod.hrl"). %% needed for the #kafka_message record definition
-include("../include/log.hrl").


%  Behavioural functions
-export([
    start/3,
    init/2,
    handle_message/2
]).


%% ====================================================================
%% Behavioural functions
%% ====================================================================

start(ClientId, GroupId, ControlTopic) ->
    %% commit offsets to kafka every 5 seconds
    GroupConfig = [{offset_commit_policy, commit_to_kafka_v2},
                   {offset_commit_interval_seconds, 5}
                  ],
    ConsumerConfig = [{begin_offset, latest}],
    GroupSubscriberConfig = #{
        client          => ClientId,
        group_id        => GroupId,
        topics          => [ControlTopic],
        cb_module       => ?MODULE,
        init_data       => [],
        message_type    => message,
        consumer_config => ConsumerConfig,
        group_config    => GroupConfig
    },
    brod:start_link_group_subscriber_v2(GroupSubscriberConfig).


%% brod_group_subscriber behaviour callback
init(#{
        group_id   := GroupId,
        topic      := _ControlTopic,
        partition  := _Partition,
        commit_fun := _CommitFun
    }, []) ->
    ?log_info("~p:~p - starting traffic server", [?MODULE, ?LINE]),
    {ok, #{
        group_id => GroupId
        }
    }.


%% brod_group_subscriber behaviour callback
handle_message(#kafka_message{} = Message, State) ->
    NewState = process_control_messages([Message], State),
    {ok, ack, NewState};

handle_message(#kafka_message_set{} = MessageSet, State) ->
    % #kafka_message_set{messages = Messages} = MessageSet,
    NewState = process_control_messages(MessageSet, State),
    {ok, ack, NewState}.



%% ====================================================================
%% Internal functions
%% ====================================================================

process_control_messages(Messages, State) ->
    NewState = 
        lists:foldl(
            fun (#kafka_message{key = K, value = V}, S) ->
                    ?log_info("~p:~p - fetched key: ~p", [?MODULE, ?LINE, K]),
                    ?log_info("~p:~p - fetched value: ~p", [?MODULE, ?LINE, V]),
                    ControlData = jsx:decode(V, [return_maps]),
                    NewS = process_control_message(ControlData, S),
                    NewS;
                (UnexpectedMessage, S) ->
                    ?log_warning("~p:~p - Ignoring message from control topic: ~p",
                                [?MODULE, ?LINE, UnexpectedMessage]),
                    S
            end,
            State,
            Messages),
    NewState.
    
%% -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

process_control_message(
    #{
      <<"params">> := ParamsMap
     },
     State
  ) ->
      %TODO search the correct sim, by equalizing ApplicationGroupId and GroupId
  
      #{
          <<"source_topic">> := SourceTopicStr,
          <<"target_topic">> := TargetTopicStr,
          <<"application_group_id">> := ApplicationGroupId,
          <<"produce_uniform_mps_global">> := ProduceUniformMPSGlobalStr,
          <<"number_of_messages">> := NumberOfMessages
          } = ParamsMap,
    
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
        
        %%   (((  dynamic params setting here  )))   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        dandelion:set_conf({produce_uniform_mps, TargetTopicStr}, ProduceUniformMPS),
  
        State2   = maps:merge(State, ParamsMap),
        NewState = maps:put(<<"produce_uniform_mps">>, ProduceUniformMPS, State2),
        NewState;
  
  process_control_message(
    #{
        <<"application_group_id">> := _ApplicationGroupId, 
        <<"command">>              := <<"start">> 
    },
    State
) ->
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
    NewState = maps:put(status, <<"started">>, State),
    NewState;


process_control_message(
    #{
        <<"application_group_id">> := _ApplicationGroupId, 
        <<"command">>              := <<"stop">> 
    },
    State
) ->
    %TODO search the correct sim, by equalizing ApplicationGroupId and GroupId

    ?log_info("~p:~p - stop_traffic", [?MODULE, ?LINE]),
    dandelion:stop_traffic(),
    NewState = maps:put(state, <<"stopped">>, State),
    NewState;

process_control_message(
    #{
        <<"application_group_id">> := _ApplicationGroupId, 
        <<"command">>              := <<"pause">> 
    },
    State
) ->
    %TODO search the correct sim, by equalizing ApplicationGroupId and GroupId

    ?log_info("~p:~p - pause_traffic", [?MODULE, ?LINE]),
    dandelion:pause_traffic(),
    NewState = maps:put(state, <<"paused">>, State),
    NewState;

process_control_message(UnexpectedMessage, State) ->
    ?log_error("~p:~p - Unexpected Message from control topic: ~p", [?MODULE, ?LINE, UnexpectedMessage]),
    State.
    

%% -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

