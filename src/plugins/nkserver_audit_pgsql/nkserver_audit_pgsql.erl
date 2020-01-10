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

-module(nkserver_audit_pgsql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([init/1, store/3, search/3, aggregate/3]).
-export([get_pgsql_srv/1]).

-define(LLOG(Type, Txt, Args), lager:Type("NkSERVER AUDIT PGSQL "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% API
%% ===================================================================


%% @doc
init(PgSrvId) ->
    init(PgSrvId, 10).


%% @private
init(PgSrvId, Tries) when Tries > 0 ->
    case nkactor_store_pgsql:query(PgSrvId, <<"SELECT uid FROM audit LIMIT 1">>) of
        {ok, _, _} ->
            ok;
        {error, field_unknown} ->
            Flavour = nkserver:get_cached_config(PgSrvId, nkpgsql, flavour),
            lager:warning("NkSERVER AUDIT: database not found: Creating it (~p)", [Flavour]),
            case nkpgsql:query(PgSrvId, create_database_query(Flavour)) of
                {ok, _, _} ->
                    ok;
                {error, Error} ->
                    lager:error("NkSERVER AUDIT: Could not create database: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            lager:warning("NkSERVER AUDIT: could not create database: ~p (~p tries left)", [Error, Tries]),
            timer:sleep(1000),
            init(PgSrvId, Tries-1)
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
            namespace TEXT NOT NULL,
            \"group\" TEXT,
            resource TEXT,
            target TEXT,
            level SMALLINT NOT NULL,
            reason TEXT,
            data JSONB,
            metadata JSONB,
            path TEXT NOT NULL
        );
        CREATE INDEX date_idx on audit (date, app, \"group\", resource, path);
        CREATE INDEX app_idx on audit (app, \"group\", resource, date, path);
        CREATE INDEX data_idx on audit USING gin(data);
        CREATE INDEX metadata_idx on audit USING gin(metadata);
        COMMIT;
    ">>.



%% @doc
store(SrvId, Audits, _Opts) ->
    Values = update_values(Audits, []),
    Query = [
        <<
            "INSERT INTO audit "
            "(uid,date,app,namespace,\"group\",resource,target,level,reason,data,metadata,path) "
            "VALUES ">>, nklib_util:bjoin(Values), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, _, SaveMeta} ->
            {ok, SaveMeta};
        {error, Error} ->
            {error, Error}
    end.


search(SrvId, Spec, _Opts) ->
    From = maps:get(from, Spec, 0),
    Size = maps:get(size, Spec, 10),
    Totals = maps:get(get_total, Spec, false),
    SQLFilters = nkserver_audit_pgsql_sql:filters(Spec),
    SQLSort = nkserver_audit_pgsql_sql:sort(Spec),

    % We could use SELECT COUNT(*) OVER(),src,uid... but it doesn't work if no
    % rows are returned

    Query = [
        case Totals of
            true ->
                [
                    <<"SELECT COUNT(*) FROM audit">>,
                    SQLFilters,
                    <<";">>
                ];
            false ->
                []
        end,
        nkserver_audit_pgsql_sql:select(Spec),
        SQLFilters,
        SQLSort,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    query(SrvId, Query, #{result_fun=> fun pgsql_audits/2}).


%% @doc
aggregate(SrvId, nkserver_audit_apps, Opts) ->
    Namespace = maps:get(namespace, Opts, <<>>),
    Deep = maps:get(deep, Opts, true),
    Query = [
        <<"SELECT \"app\", COUNT(\"app\") FROM audit">>,
        <<" WHERE ">>, filter_path(Namespace, Deep),
        <<" GROUP BY \"app\";">>
    ],
    query(SrvId, Query, #{result_fun=>fun pgsql_aggregate/2}).



%% ===================================================================
%% Internal
%% ===================================================================

%% @doc
get_pgsql_srv(ActorSrvId) ->
    nkserver:get_cached_config(ActorSrvId, nkserver_audit_pgsql, pgsql_service).


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
        namespace := Namespace,
        level := Level,
        reason := Reason,
        data := Data,
        metadata := Meta
    } = Audit,
    Group = maps:get(group, Audit, null),
    Res = maps:get(resource, Audit, null),
    Target = maps:get(target, Audit, null),
    Path = make_rev_path(Namespace),
    Fields1 = [
        quote(UID),
        quote(Date),
        quote(App),
        quote(Namespace),
        quote(Group),
        quote(Res),
        quote(Target),
        Level,
        quote(Reason),
        quote(Data),
        quote(Meta),
        quote(Path)
    ],
    Fields2 = <<$(, (nklib_util:bjoin(Fields1))/binary, $)>>,
    update_values(Rest, [Fields2|Acc]).


%% @private
quote(Term) ->
    nkpgsql_util:quote(Term).


%% @private
pgsql_audits(Result, Meta) ->
    #{pgsql:=#{time:=Time}} = Meta,
    {Rows, Meta2} = case Result of
        [{{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, time=>Time}};
        [{{select, 1}, [{Total}], _}, {{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, total=>Total, time=>Time}}
    end,
    Actors = lists:map(
        fun
            ({UID, Date, App, Ns, Group, Res, Target, Level, Reason}) ->
                #{
                    uid => UID,
                    date => Date,
                    app => App,
                    namespace => Ns,
                    group => Group,
                    resource => Res,
                    target => Target,
                    level => Level,
                    reason => Reason
                };
            ({UID, Date, App, Ns, Group, Res, Target, Level, Reason, {jsonb, Data}, {jsonb, MetaD}}) ->

                #{
                    uid => UID,
                    date => Date,
                    app => App,
                    namespace => Ns,
                    group => Group,
                    resource => Res,
                    target => Target,
                    level => Level,
                    reason => Reason,
                    data => nklib_json:decode(Data),
                    metadata => nklib_json:decode(MetaD)
                }
        end,
        Rows),
    {ok, Actors, Meta2}.


%% @private
pgsql_aggregate([{{select, _Size}, Rows, _OpMeta}], Meta) ->
    case (catch maps:from_list(Rows)) of
        {'EXIT', _} ->
            {error, aggregation_invalid};
        Map ->
            {ok, Map, Meta}
    end.


make_rev_path(Namespace) ->
    Parts = make_rev_parts(Namespace),
    nklib_util:bjoin(Parts, $.).


%% @private
make_rev_parts(Namespace) ->
    case to_bin(Namespace) of
        <<>> ->
            [];
        Namespace2 ->
            lists:reverse(binary:split(Namespace2, <<$.>>, [global]))
    end.

%% @private
filter_path(<<>>, true) ->
    [<<"TRUE">>];

filter_path(Namespace, Deep) ->
    Path = nkactor_lib:make_rev_path(Namespace),
    case Deep of
        true ->
            [<<"(path LIKE ">>, quote(<<Path/binary, "%">>), <<")">>];
        false ->
            [<<"(path = ">>, quote(Path), <<")">>]
    end.

to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

