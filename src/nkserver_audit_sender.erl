%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


-module(nkserver_audit_sender).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([store/2, do_store/2, pause/1, get_total/1]).
-export([start_link/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
    handle_info/2]).
-export([split/2]).

-include_lib("nkserver/include/nkserver.hrl").

-define(BATCH, 1000).

%% ===================================================================
%% Public
%% ===================================================================

%% @doc
start_link(SrvId) ->
    gen_server:start_link(?MODULE, [SrvId], []).


%% @doc
store(SrvId, Audit) when is_map(Audit) ->
    case nkserver_audit:parse(Audit) of
        {ok, [Audit2]} ->
            do_store(SrvId, Audit2);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
do_store(SrvId, Audit) when is_map(Audit) ->
    case nklib_util:do_config_get({?MODULE, SrvId}, undefined) of
        Pid when is_pid(Pid) ->
            gen_server:cast(Pid, {new_audit, Audit});
        _ ->
            ok
    end.


%% @doc
get_total(SrvId) ->
    case nklib_util:do_config_get({?MODULE, SrvId}, undefined) of
        Pid when is_pid(Pid) ->
            gen_server:call(Pid, get_total);
        _ ->
            {error, audit_not_started}
    end.


%% @doc
pause(Boolean) ->
    nklib_util:do_config_put(nkserver_audit_pause_sender, Boolean).


%% ===================================================================
%% gen_server
%% ===================================================================

-record(state, {
    srv :: nkserver:id(),
    interval :: integer() | undefined,
    audits = [] :: [nkserver_audit:audit()],
    total = 0 :: integer()
}).


%% @private
init([SrvId]) ->
    nklib_util:do_config_put({?MODULE, SrvId}, self()),
    pause(false),
    Time = 5000,
    State = #state{srv=SrvId, interval = Time},
    lager:notice("Starting NkSERVER AUDIT Sender (srv:~s, interval:~p)", [SrvId, Time]),
    self() ! send_audits,
    {ok, State}.


%% @private
handle_call(get_total, _From, #state{total=Total}=State) ->
    {reply, {ok, Total}, State};

handle_call(Msg, _From, State) ->
    lager:error("Received unexpected call at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_cast({new_audit, Audit}, #state{audits=Audits, total=Total}=State) ->
    State = State#state{audits=[Audit|Audits], total=Total+1},
    case Total >= ?BATCH of
        true ->
            {noreply, send_audits(State)};
        false ->
            {noreply, State}
    end;

handle_cast(Msg, State) ->
    lager:error("Received unexpected call at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_info(send_audits, #state{interval=Time}=State) ->
    State2 = send_audits(State),
    erlang:send_after(Time, self(), send_audits),
    {noreply, State2};

handle_info(Msg, State) ->
    lager:error("Received unexpected info at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
terminate(_Reason, _State) ->
    ok.



%% ===================================================================
%% Internal
%% ===================================================================

send_audits(#state{audits=[]}=State) ->
    State;

send_audits(#state{srv=SrvId, audits=Audits, total=Total}=State) ->
    case nklib_util:do_config_get(nkserver_audit_pause_sender, false) of
        true ->
            {message_queue_len, Len} = process_info(self(), message_queue_len),
            lager:notice("Skipping sending ~p spans (~s) (waiting: ~p)", [Total, SrvId, Len]);
        _ ->
            do_send_audits(SrvId, Audits, Total)
    end,
    State#state{audits = [], total = 0}.


%% @private
do_send_audits(SrvId, Audits, Total) ->
    try
        case ?CALL_SRV(SrvId, audit_store, [SrvId, Audits, #{}]) of
            {ok, _Meta} ->
                {message_queue_len, QueueLen} = process_info(self(), message_queue_len),
                case Total >= ?BATCH of
                    true ->
                        lager:debug("Sent ~p/~p AUDITS (waiting ~p)", [?BATCH, Total, QueueLen]);
                    false ->
                        lager:debug("Sent ~p AUDITS (waiting ~p)", [Total, QueueLen])
                end;
            Error ->
                lager:warning("Error sending audits: ~p", [Error])
        end
    catch
        Class:Reason:Stack ->
            lager:warning("Exception calling audit_store: ~p ~p (~p)", [Class, Reason, Stack])
    end.


%% @private
split(List, Max) ->
    split(List, 0, Max, []).


%% @private
split([], _Pos, _Max, Acc) ->
    {Acc, []};

split([Next|Rest], Pos, Max, Acc) when Pos < Max ->
    split(Rest, Pos+1, Max, [Next|Acc]);

split([Next|Rest], _Pos, _Max, Acc) ->
    {Acc, [Next|Rest]}.
