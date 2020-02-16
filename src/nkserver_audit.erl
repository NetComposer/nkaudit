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


%% @doc
-module(nkserver_audit).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([store/2, store/3, get_apps/1, search/3]).
-export([aggregate/3, parse/1]).

-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Types
%% ===================================================================


-type level() :: nkserver_trace:level().

-type audit() ::
    #{
        uid := binary(),                % Generated
        node := binary(),               % Generated
        date := binary(),               % Generated
        app := binary(),                % Mandatory
        namespace => binary(),          % Default ""
        group => binary(),
        resource => binary(),
        type => binary(),               % Mandatory
        target => binary(),
        level => 1..7 | level(),        % Default 2
        reason => binary(),             % Default ""
        data => map(),
        metadata => #{
            count => integer(),
            first_date => binary(),
            trace_id => binary(),
            tags => [{binary(), binary()}]
        }
}.

-type store_opts() :: #{}.

-type agg_type() :: any().

%% ===================================================================
%% API
%% ===================================================================

%% @doc
-spec store(nkserver:id(), audit()|[audit()]) ->
    ok | {error, term()}.

store(SrvId, Audits) ->
    store(SrvId, Audits, #{}).


%% @doc Stores an audit sync with the database
-spec store(nkserver:id(), audit()|[audit()], store_opts()) ->
    ok | {error, term()}.

store(SrvId, Audits, Opts) ->
    case parse(Audits) of
        {ok, Audits2} ->
            case ?CALL_SRV(SrvId, audit_store, [SrvId, Audits2, Opts]) of
                {ok, _Meta} ->
                    ok;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets all apps storing audits
get_apps(SrvId) ->
    aggregate(SrvId, nkserver_audit_apps, #{}).


%% @doc Generic search
-spec search(nkserver:id(), nkserver_audit_search:spec(), nkserver_audit_search:opts()) ->
    {ok, [audit()], Meta::map()} | {error, term()}.

search(SrvId, SearchSpec, SearchOpts) ->
    case nkserver_audit_search:parse_spec(SearchSpec, SearchOpts) of
        {ok, SearchSpec2} ->
            ?CALL_SRV(SrvId, audit_search, [SrvId, SearchSpec2, SearchOpts]);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Generic aggregation
-spec aggregate(nkserver:id(), agg_type(), map()) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

aggregate(SrvId, AggType, Opts) ->
    ?CALL_SRV(SrvId, audit_aggregate, [SrvId, AggType, Opts]).


%% @doc
-spec parse(audit()|[audit()]) ->
    {ok, [audit()]} | {error, term()}.

parse(Audit) when is_map(Audit) ->
    parse([Audit]);

parse(Audits) when is_list(Audits) ->
    parse(Audits, []).


%% ===================================================================
%% Internal
%% ===================================================================


%% @private
parse([], Acc) ->
    {ok, lists:reverse(Acc)};

parse([Audit|Rest], Acc) ->
    Syntax = #{
        uid => binary,
        node => binary,
        date => binary,
        app => binary,
        namespace => binary,
        group => [{atom, [null]}, binary],
        resource => [{atom, [null]}, binary],
        type => binary,
        target => [{atom, [null]}, binary],
        level => [{integer, 1, 7}, {atom, [debug,trace,info,event,notice,warning,error]}],
        reason => binary,
        data => map,
        metadata => map,
        '__mandatory' => [app, type],
        '__defaults' => #{
            namespace => <<>>,
            level => 2,
            reason => <<>>,
            data => #{},
            metadata => #{}
        }
    },
    case nklib_syntax:parse(Audit, Syntax) of
        {ok, Audit2, _} ->
            Audit3 = case maps:is_key(uid, Audit2) of
                true ->
                    Audit2;
                false ->
                    Audit2#{uid => make_uid()}
            end,
            Audit4 = case maps:is_key(date, Audit3) of
                true ->
                    Audit3;
                false ->
                    Audit3#{date => nklib_date:now_3339(usecs)}
            end,
            Audit5 = case maps:is_key(node, Audit4) of
                true ->
                    Audit4;
                false ->
                    Audit4#{node => atom_to_binary(node(), utf8)}
            end,
            Audit6 = case Audit5 of
                #{level:=Level} ->
                    case is_atom(Level) of
                        true ->
                            Level2 = nkserver_trace:name_to_level(Level),
                            Audit5#{level:=Level2};
                        false ->
                            Audit5
                    end;
                _ ->
                    Audit5#{level=>2}
            end,
            parse(Rest, [Audit6|Acc]);
        {error, Error} ->
            {error, Error}
    end.


make_uid() ->
    Time = nklib_date:now_bin(msecs),   % 9 bytes
    <<UUID:18/binary, _/binary>> = nklib_util:luid(),
    <<Time/binary, UUID/binary>>.
