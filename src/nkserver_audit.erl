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
-export([store/3, get_apps/1, search/3]).
-export([aggregate/3, parse/1]).

-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Types
%% ===================================================================


-type level() :: debug | info | notice | warning | error.

-type audit() ::
    #{
        uid := binary(),
        date := binary(),
        app := binary(),
        namespace => binary(),
        group => binary(),
        type => binary(),
        level => 1..7 | level(),
        trace => binary(),
        id => binary(),
        id2 => binary(),
        id3 => binary(),
        msg => binary(),
        data => map()
}.

-type store_opts() :: #{}.

-type agg_type() :: any().

%% ===================================================================
%% API
%% ===================================================================

%% @doc
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


%% @doc
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


%% @doc
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
        date => binary,
        app => binary,
        namespace => binary,
        group => binary,
        type => binary,
        level => [{integer, 1, 7}, {atom, [debug,info,notice,warning,error]}],
        trace => binary,
        id => binary,
        id2 => binary,
        id3 => binary,
        msg => binary,
        data => map,
        '__mandatory' => [app],
        '__defaults' => #{
            namespace => <<>>,
            level => debug,
            msg => <<>>,
            data => #{}
        }
    },
    case nklib_syntax:parse(Audit, Syntax) of
        {ok, Audit2, []} ->
            Audit3 = case maps:is_key(uid, Audit2) of
                true ->
                    Audit2;
                false ->
                    Audit2#{uid => nklib_util:luid()}
            end,
            Audit4 = case maps:is_key(date, Audit3) of
                true ->
                    Audit3;
                false ->
                    Audit3#{date => nklib_date:now_3339(usecs)}
            end,
            Audit5 = case Audit4 of
                #{level:=Level} when is_atom(Level) ->
                    Level2 = case Level of
                        debug -> 1;
                        info -> 2;
                        notice -> 3;
                        warning -> 4;
                        error -> 5
                    end,
                    Audit4#{level:=Level2};
                _ ->
                    Audit4
            end,
            parse(Rest, [Audit5|Acc]);
        {ok, _, [Field|_]} ->
            {error, {field_unknown, Field}};
        {error, Error} ->
            {error, Error}
    end.

