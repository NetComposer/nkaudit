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

%% @doc Default plugin callbacks
-module(nkaudit_pgsql_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').


-export([status/1]).
-export([audit_db_init/1, audit_store/3, audit_aggregate/3, audit_search/3]).

-include_lib("nkserver/include/nkserver.hrl").

status({pgsql_error, Error}) -> {"PgSQL Error: ~p", [Error]};
status(_) -> continue.



%% ===================================================================
%% Offered callbacks
%% ===================================================================


-type continue() :: nkserver_callbacks:continue().


%% @doc Called after the core has initialized the database
-spec audit_db_init(nkserver:id()) ->
    ok | {error, term()} | continue().

audit_db_init(_SrvId) ->
    ok.



%% ===================================================================
%% Persistence callbacks
%% ===================================================================


%% @doc Must create a new audit on disk. Should fail if already present
-spec audit_store(nkserver:id(), [nkaudit:audit()], nkaudit:store_opts()) ->
    {ok, Meta::map()} | {error, uniqueness_violation|term()} | continue().

audit_store(SrvId, Audits, Opts) ->
    call(SrvId, store, Audits, Opts).


%% @doc
-spec audit_search(nkserver:id(), nkaudit_search:spec(), nkaudit_search:opts()) ->
    {ok, [nkaudit:audit()], map()} | {error, term()}.

audit_search(SrvId, Spec, Opts) ->
    call(SrvId, search, Spec, Opts).


%% @doc
-spec audit_aggregate(nkserver:id(), nkaudit:agg_type(), nkaudit:agg_opts()) ->
    {ok, [nkaudit:audit()], map()} | {error, term()}.

audit_aggregate(SrvId, Type, Opts) ->
    call(SrvId, aggregate, Type, Opts).


%% ===================================================================
%% Internal
%% ===================================================================

%% @private
call(SrvId, Op, Arg, Opts) ->
    case nkaudit_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Reply = nkaudit_pgsql:Op(PgSrvId, Arg, Opts),
            reply(Reply)
    end.


%% @private
reply({error, Error}) ->
    {error, {pgsql_error, Error}};

reply(Other) ->
    Other.


