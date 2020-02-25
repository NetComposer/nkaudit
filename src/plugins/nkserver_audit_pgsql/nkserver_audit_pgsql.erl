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
-export([init/2, store/4, search/4, aggregate/4]).
-export([get_pgsql_srv/1, get_table/1]).

-define(LLOG(Type, Txt, Args), lager:Type("NkSERVER AUDIT PGSQL "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% API
%% ===================================================================


%% @doc
init(PgSrvId, Table) ->
    init(PgSrvId, Table, 10).


%% @private
init(PgSrvId, Table, Tries) when Tries > 0 ->
    case nkactor_store_pgsql:query(PgSrvId, <<"SELECT uid FROM ", Table/binary, " LIMIT 1">>) of
        {ok, _, _} ->
            ok;
        {error, field_unknown} ->
            Flavour = nkserver:get_cached_config(PgSrvId, nkpgsql, flavour),
            lager:warning("NkSERVER AUDIT: table (~s) not found: Creating it (~p)", [Table, Flavour]),
            case nkpgsql:query(PgSrvId, create_database_query(Flavour, Table)) of
                {ok, _, _} ->
                    ok;
                {error, Error} ->
                    lager:error("NkSERVER AUDIT: Could not create table: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            lager:warning("NkSERVER AUDIT: could not create table: ~p (~p tries left)", [Error, Tries]),
            timer:sleep(1000),
            init(PgSrvId, Table, Tries-1)
    end;

init(_SrvId, _Table, _Tries) ->
    {error, database_not_available}.


%% @private
create_database_query(postgresql, Table) ->
    <<"
        -- Comment
        BEGIN;
        CREATE TABLE ", Table/binary, " (
            date TEXT NOT NULL,
            app TEXT NOT NULL,
            \"group\" TEXT,
            resource TEXT,
            type TEXT NOT NULL,
            reason TEXT,
            target TEXT,
            data JSONB,
            metadata JSONB,
            namespace TEXT NOT NULL,
            level SMALLINT NOT NULL,
            uid TEXT PRIMARY KEY NOT NULL,
            node TEXT NOT NULL,
            path TEXT NOT NULL
        );
        CREATE INDEX ", Table/binary, "_date_idx on ", Table/binary, " (date, \"group\", resource, path);
        CREATE INDEX ", Table/binary, "_app_idx on ", Table/binary, " (\"group\", resource, date, path);
        CREATE INDEX ", Table/binary, "_date_type_idx on ", Table/binary, " (date, type, path);
        CREATE INDEX ", Table/binary, "_type_date_idx on ", Table/binary, " (type, date, path);
        CREATE INDEX ", Table/binary, "_data_idx on ", Table/binary, " USING gin(data);
        CREATE INDEX ", Table/binary, "_metadata_idx on ", Table/binary, " USING gin(metadata);
        COMMIT;
    ">>.


%% @doc
store(PgSrvId, Table, Audits, _Opts) ->
    Values = update_values(Audits, []),
    Query = [
        <<
            "INSERT INTO ", Table/binary, " ",
             "(date,app,\"group\",resource,type,reason,target,data,metadata,namespace,level,uid,node,path) "
            "VALUES ">>, nklib_util:bjoin(Values), <<";">>
    ],
    case query(PgSrvId, Query) of
        {ok, _, SaveMeta} ->
            {ok, SaveMeta};
        {error, Error} ->
            {error, Error}
    end.


search(SrvId, Table, Spec, _Opts) ->
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
                    <<"SELECT COUNT(*) FROM ", Table/binary, " ">>,
                    SQLFilters,
                    <<";">>
                ];
            false ->
                []
        end,
        nkserver_audit_pgsql_sql:select(Table, Spec),
        SQLFilters,
        SQLSort,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    query(SrvId, Query, #{result_fun=> fun pgsql_audits/2}).


%% @doc
aggregate(SrvId, Table, nkserver_audit_apps, Opts) ->
    Namespace = maps:get(namespace, Opts, <<>>),
    Deep = maps:get(deep, Opts, true),
    Query = [
        <<"SELECT \"app\", COUNT(\"app\") FROM ", Table/binary>>,
        <<" WHERE ">>, filter_path(Namespace, Deep),
        <<" GROUP BY \"app\";">>
    ],
    query(SrvId, Query, #{result_fun=>fun pgsql_aggregate/2}).



%% ===================================================================
%% Internal
%% ===================================================================

%% @doc
get_pgsql_srv(SrvId) ->
    nkserver:get_cached_config(SrvId, nkserver_audit_pgsql, pgsql_service).

%% @doc
get_table(SrvId) ->
    nkserver:get_cached_config(SrvId, nkserver_audit_pgsql, table).


%% @doc Performs a query. Must use the PgSQL service
-spec query(nkserver:id(), binary()|nkpgsql:query_fun()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query) ->
    query(SrvId, Query, #{}).


%% @doc Performs a query. Must use the PgSQL service
-spec query(nkserver:id(), binary()|nkpgsql:query_fun(), nkpgsql:query_meta()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query, QueryMeta) ->
    nkserver_trace:tags(#{<<"query.sql">>=>list_to_binary([Query])}),
    nkpgsql:query(SrvId, Query, QueryMeta).


%% @private
update_values([], Acc) ->
    Acc;

update_values([Audit|Rest], Acc) ->
    #{
        uid := UID,
        date := Date,
        node := Node,
        app := App,
        namespace := Namespace,
        type := Type,
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
        quote(Date),
        quote(App),
        quote(Group),
        quote(Res),
        quote(Type),
        quote(Reason),
        quote(Target),
        quote(Data),
        quote(Meta),
        quote(Namespace),
        Level,
        quote(UID),
        quote(Node),
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
            ({Date, App, Group, Res, Type, Reason, Target, Ns, Level, UID, Node}) ->
                #{
                    uid => UID,
                    node => Node,
                    date => Date,
                    app => App,
                    namespace => Ns,
                    group => Group,
                    resource => Res,
                    type => Type,
                    target => Target,
                    level => Level,
                    reason => Reason
                };
            ({Date, App, Group, Res, Type, Reason, Target, Ns, Level, UID, Node, {jsonb, Data}, {jsonb, MetaD}}) ->

                #{
                    uid => UID,
                    date => Date,
                    node => Node,
                    app => App,
                    namespace => Ns,
                    group => Group,
                    resource => Res,
                    type => Type,
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

