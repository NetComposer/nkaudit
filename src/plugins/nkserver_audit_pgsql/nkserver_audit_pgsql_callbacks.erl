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
-module(nkserver_audit_pgsql_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').


-export([status/1]).
-export([audit_db_init/1, audit_store/3, audit_aggregate/3, audit_search/3]).

-include_lib("nkserver/include/nkserver.hrl").

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
-spec audit_store(nkserver:id(), [nkserver_audit:audit()], nkserver_audit:store_opts()) ->
    {ok, Meta::map()} | {error, term()} | continue().

audit_store(SrvId, Audits, Opts) ->
    call(SrvId, store, Audits, Opts).


%% @doc
-spec audit_search(nkserver:id(), nkserver_audit_search:spec(), nkserver_audit_search:opts()) ->
    {ok, [nkserver_audit:audit()], map()} | {error, term()}.

audit_search(SrvId, Spec, Opts) ->
    call(SrvId, search, Spec, Opts).


%% @doc
-spec audit_aggregate(nkserver:id(), nkserver_audit:agg_type(), nkserver_audit:agg_opts()) ->
    {ok, [nkserver_audit:audit()], map()} | {error, term()}.

audit_aggregate(SrvId, Type, Opts) ->
    call(SrvId, aggregate, Type, Opts).


%% ===================================================================
%% Internal
%% ===================================================================

%% @private
call(SrvId, Op, Arg, Opts) ->
    case nkserver_audit_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() ->
                Table = nkserver_audit_pgsql:get_table(SrvId),
                nkserver_audit_pgsql:Op(PgSrvId, Table, Arg, Opts)
            end,
            new_span(SrvId, PgSrvId, Op, Fun)
    end.


%% @private
reply({error, Error}) ->
    {error, {pgsql_error, Error}};

reply(Other) ->
    Other.


%% @private
new_span(SrvId, PgSrvId, Op, Fun) ->
    Opts = #{pgsql_app => PgSrvId },
    Fun2 = fun() -> reply(Fun()) end,
    nkserver_trace:new_span(SrvId, {nkactor_store_pgsql, Op}, Fun2, Opts).

