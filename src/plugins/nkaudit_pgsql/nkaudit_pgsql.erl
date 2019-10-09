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

-module(nkaudit_pgsql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_pgsql_srv/1]).
-export([query/2, query/3]).
-export([init/2, store/3]).

-define(LLOG(Type, Txt, Args), lager:Type("NkAUDIT PGSQL "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% API
%% ===================================================================


%% @private
init(SrvId, Tries) when Tries > 0 ->
    case nkactor_store_pgsql:query(SrvId, <<"SELECT uid FROM audit LIMIT 1">>) of
        {ok, _, _} ->
            ok;
        {error, field_unknown} ->
            Flavour = nkserver:get_cached_config(SrvId, nkpgsql, flavour),
            lager:warning("NkAUDIT: database not found: Creating it (~p)", [Flavour]),
            case nkpgsql:query(SrvId, create_database_query(Flavour)) of
                {ok, _, _} ->
                    ok;
                {error, Error} ->
                    lager:error("NkAUDIT: Could not create database: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            lager:warning("NkAUDIT: could not create database: ~p (~p tries left)", [Error, Tries]),
            timer:sleep(1000),
            init(SrvId, Tries-1)
    end;

init(_SrvId, _Tries) ->
    {error, database_not_available}.



%% @private
create_database_query(postgresql) ->
    <<"
        -- Comment
        BEGIN;
        CREATE TABLE audit (
            uid TEXT PRIMARY KEY NOT NULL,
            date TEXT NOT NULL,
            app TEXT NOT NULL,
            \"group\" TEXT,
            type TEXT,
            level SMALLINT NOT NULL,
            trace TEXT,
            id TEXT NOT NULL,
            id2 TEXT,
            id3 TEXT,
            msg TEXT,
            data JSONB
        );
        CREATE INDEX date_idx on audit (date, app, \"group\", type);
        CREATE INDEX app_idx on audit (app, \"group\", type, date);
        CREATE INDEX data_idx on audit USING gin(data);
        COMMIT;
    ">>.



%% @doc
store(SrvId, Audits, _Opts) ->
    Values = update_values(Audits, []),
    Query = [
        <<"INSERT INTO audit (uid,date,app,\"group\",type,level,trace,id,id2,id3,msg,data) ">>,
        <<"VALUES ">>, nklib_util:bjoin(Values), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, _, SaveMeta} ->
            {ok, SaveMeta};
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% Internal
%% ===================================================================

%% @doc
get_pgsql_srv(ActorSrvId) ->
    nkserver:get_cached_config(ActorSrvId, nkaudit_pgsql, pgsql_service).


%% @doc Performs a query. Must use the PgSQL service
-spec query(nkserver:id(), binary()|nkpgsql:query_fun()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query) ->
    nkpgsql:query(SrvId, Query, #{}).


%% @doc Performs a query. Must use the PgSQL service
-spec query(nkserver:id(), binary()|nkpgsql:query_fun(), nkpgsql:query_meta()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query, QueryMeta) ->
    nkpgsql:query(SrvId, Query, QueryMeta).



%% @private
update_values([], Acc) ->
    Acc;

update_values([Audit|Rest], Acc) ->
    #{
        uid := UID,
        date := Date,
        app := App,
        level := Level,
        msg := Msg,
        data := Data
    } = Audit,
    Group = maps:get(group, Audit, null),
    Type = maps:get(type, Audit, null),
    Trace = maps:get(trace, Audit, null),
    Id = maps:get(id, Audit, null),
    Id2 = maps:get(id2, Audit, null),
    Id3 = maps:get(id3, Audit, null),
    Fields1 = [
        quote(UID),
        quote(Date),
        quote(App),
        quote(Group),
        quote(Type),
        Level,
        quote(Trace),
        quote(Id),
        quote(Id2),
        quote(Id3),
        quote(Msg),
        quote(Data)
    ],
    Fields2 = <<$(, (nklib_util:bjoin(Fields1))/binary, $)>>,
    update_values(Rest, [Fields2|Acc]).


%% @private
quote(Term) ->
    nkpgsql_util:quote(Term).
