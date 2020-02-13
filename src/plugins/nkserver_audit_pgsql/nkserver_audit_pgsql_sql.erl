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


%% @doc SQL utilities for stores to use
-module(nkserver_audit_pgsql_sql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([select/2, filters/1, sort/1, field_name/2]).
-import(nkactor_store_pgsql, [quote/1, filter_path/2]).


%% ===================================================================
%% Select
%% ===================================================================


%% @private
select(Table, Params) ->
    [
        <<"SELECT uid,date,app,namespace,\"group\",resource,type,target,level,reason">>,
        case maps:get(get_data, Params, false) of
            true ->
                <<",data,metadata">>;
            false ->
                []
        end,
        <<" FROM ", Table/binary>>
    ].



%% ===================================================================
%% Filters
%% ===================================================================


%% @private
filters(#{namespace:=Namespace}=Params) ->
    Flavor = maps:get(flavor, Params, #{}),
    Filters = maps:get(filter, Params, #{}),
    AndFilters1 = expand_filter(maps:get('and', Filters, []), []),
    AndFilters2 = make_filter(AndFilters1, Flavor, []),
    OrFilters1 = expand_filter(maps:get('or', Filters, []), []),
    OrFilters2 = make_filter(OrFilters1, Flavor, []),
    OrFilters3 = nklib_util:bjoin(OrFilters2, <<" OR ">>),
    OrFilters4 = case OrFilters3 of
        <<>> ->
            [];
        _ ->
            [<<$(, OrFilters3/binary, $)>>]
    end,
    NotFilters1 = expand_filter(maps:get('not', Filters, []), []),
    NotFilters2 = make_filter(NotFilters1, Flavor, []),
    NotFilters3 = case NotFilters2 of
        <<>> ->
            [];
        _ ->
            [<<"(NOT ", F/binary, ")">> || F <- NotFilters2]
    end,
    Deep = maps:get(deep, Params, false),
    PathFilter = list_to_binary(filter_path(Namespace, Deep)),
    FilterList = [PathFilter | AndFilters2 ++ OrFilters4 ++ NotFilters3],
    Where = nklib_util:bjoin(FilterList, <<" AND ">>),
    [<<" WHERE ">>, Where].


%% @private
expand_filter([], Acc) ->
    Acc;

expand_filter([#{field:=Field, value:=Value}=Term|Rest], Acc) ->
    Op = maps:get(op, Term, eq),
    Type = maps:get(type, Term, string),
    Value2 = case Type of
        _ when Op==exists ->
            to_boolean(Value);
        string when Op==values, is_list(Value) ->
            [to_bin(V) || V <- Value];
        string ->
            to_bin(Value);
        integer when Op==values, is_list(Value) ->
            [to_integer(V) || V <- Value];
        integer ->
            to_integer(Value);
        boolean when Op==values, is_list(Value) ->
            [to_boolean(V) || V <- Value];
        boolean ->
            to_boolean(Value);
        object ->
            Value
    end,
    expand_filter(Rest, [{Field, Op, Value2, Type}|Acc]).



%% @private
make_filter([], _Flavor, Acc) ->
    Acc;

%% CR only indexed on '@>'
make_filter([{<<"data.", Field/binary>>, eq, Value, _}|Rest], Flavor, Acc) ->
    Json = field_value(Field, Value),
    Filter = [<<"(data @> '">>, Json, <<"')">>],
    make_filter(Rest, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{Field, _Op, _Val, object} | Rest], Flavor, Acc) ->
    lager:warning("using invalid object operator at ~p: ~p", [?MODULE, Field]),
    make_filter(Rest, Flavor, Acc);

% NOT INDEXED for data, metadata using CR
make_filter([{Field, exists, Bool, Value}|Rest], Flavor, Acc) ->
    Not = case Bool of true -> <<"NOT">>; false -> <<>> end,
    Field2 = field_name(Field, Value),
    Filter = [<<"(">>, Field2, <<" IS ">>, Not, <<" NULL)">>],
    make_filter(Rest, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{Field, prefix, Val, string}|Rest], Flavor, Acc) ->
    Field2 = field_name(Field, string),
    Filter = [$(, Field2, <<" LIKE ">>, quote(<<Val/binary, $%>>), $)],
    make_filter(Rest, Flavor, [list_to_binary(Filter)|Acc]);

make_filter([{Field, values, ValList, Type}|Rest], Flavor, Acc) when is_list(ValList) ->
    Values = nklib_util:bjoin([quote(Val) || Val <- ValList], $,),
    Field2 = field_name(Field, Type),
    Filter = [$(, Field2, <<" IN (">>, Values, <<"))">>],
    make_filter(Rest, Flavor, [list_to_binary(Filter)|Acc]);

make_filter([{Field, values, Value, Type}|Rest], Flavor, Acc) ->
    make_filter([{Field, values, [Value], Type}|Rest], Flavor, Acc);

make_filter([{Field, Op, Val, Type} | Rest], Flavor, Acc) ->
    Field2 = field_name(Field, Type),
    Filter = [$(, get_op(Field2, Op, Val), $)],
    make_filter(Rest, Flavor, [list_to_binary(Filter) | Acc]).


%% @private
get_op(Field, eq, Value) -> [Field, << "=" >>, quote(Value)];
get_op(Field, ne, Value) -> [Field, <<" <> ">>, quote(Value)];
get_op(Field, lt, Value) -> [Field, <<" < ">>, quote(Value)];
get_op(Field, lte, Value) -> [Field, <<" <= ">>, quote(Value)];
get_op(Field, gt, Value) -> [Field, <<" > ">>, quote(Value)];
get_op(Field, gte, Value) -> [Field, <<" >= ">>, quote(Value)];
get_op(_Field, exists, _Value) -> [<<"TRUE">>].


%% ===================================================================
%% Sort
%% ===================================================================


%% @private
sort(Params) ->
    Flavor = maps:get(flavor, Params, #{}),
    Sort = expand_sort(maps:get(sort, Params, []), []),
    make_sort(Sort, Flavor, []).


%% @private
expand_sort([], Acc) ->
    lists:reverse(Acc);

expand_sort([#{field:=Field}=Term|Rest], Acc) ->
    Order = maps:get(order, Term, asc),
    Type = maps:get(type, Term, string),
    expand_sort(Rest, [{Order, Field, Type}|Acc]).


%% @private
make_sort([], _Flavor, []) ->
    <<>>;

make_sort([], _Flavor, Acc) ->
    [<<" ORDER BY ">>, nklib_util:bjoin(lists:reverse(Acc), $,)];

make_sort([{Order, Field, Type}|Rest], Flavor, Acc) ->
    Item = [
        field_name(Field, Type),
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end
    ],
    make_sort(Rest, Flavor, [list_to_binary(Item)|Acc]).



%% ===================================================================
%% Utilities
%% ===================================================================


%% @private
%% Extracts a field inside a JSON, it and casts it to json, string, integer o boolean
field_name(Field, Type) ->
    list_to_binary(field_name(Field, Type, [], [])).


%% @private
field_name(Field, Type, Heads, Acc) ->
    case binary:split(Field, <<".">>) of
        [Last] when Acc==[] ->
            [Last];
        [Last] ->
            finish_field_name(Type, Last, Acc);
        [Base, Rest] when Acc==[] andalso (Base == <<"metadata">> orelse Base == <<"data">>) ->
            field_name(Rest, Type, [Base|Heads], [Base, <<"->">>]);
        [Base, Rest] ->
            field_name(Rest, Type, [Base|Heads], Acc++[$', Base, $', <<"->">>])
    end.

finish_field_name(Type, Last, Acc) ->
    case Type of
        json ->
            Acc++[$', Last, $'];
        string ->
            Acc++[$>, $', Last, $'];    % '>' finishes ->>
        integer ->
            [$(|Acc] ++ [$>, $', Last, $', <<")::INTEGER">>];
        boolean ->
            [$(|Acc] ++ [$>, $', Last, $', <<")::BOOLEAN">>]
    end.


%% @private Generates a JSON based on a field
%% make_json_spec(<<"a.b.c">>) = {"a":{"b":"c"}}
field_value(Field, Value) ->
    List = binary:split(Field, <<".">>, [global]),
    Map = field_value(List++[Value]),
    nklib_json:encode(Map).


%% @private
field_value([]) -> #{};
field_value([Last]) -> Last;
field_value([Field|Rest]) -> #{Field => field_value(Rest)}.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).


%% @private
to_integer(Term) when is_integer(Term) ->
    Term;
to_integer(Term) ->
    case nklib_util:to_integer(Term) of
        error -> 0;
        Integer -> Integer
    end.

%% @private
to_boolean(Term) when is_boolean(Term) ->
    Term;
to_boolean(Term) ->
    case nklib_util:to_boolean(Term) of
        error -> false;
        Boolean -> Boolean
    end.
