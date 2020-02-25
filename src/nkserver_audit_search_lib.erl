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

%% @doc Actor Search
-module(nkserver_audit_search_lib).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([syntax/0, search_spec/2]).



syntax() ->
    #{
        domain => binary,
        deep => boolean,
        filter => {list, #{
            field => binary,
            op => {atom, [eq, ne, lt, lte, gt, gte, values, exists, prefix, range]},
            bool => {atom, ['and', 'or', 'not']},
            type => {atom, [string, string_null, integer, boolean]},
            value => binary,
            '__mandatory' => [field],
            '__defaults' => #{bool => 'and'}
        }},
        from => integer,
        size => integer,
        sort => {list, binary},
        get_data => boolean,
        get_total => boolean,
        '__defaults' => #{
            deep => true,
            size => 100,
            get_total => false,
            get_data => true
        }
    }.


%% @doc
search_spec(Spec, Fields) ->
    Fields2 = Fields#{
        <<"id">> => <<"name">>
    },
    search_spec(maps:to_list(Spec), Fields2, #{}).


%% @private
search_spec([], _Fields, Spec) ->
    {ok, Spec};

search_spec([{Key, Value}|Rest], Fields, Spec)
    when Key==namespace; Key==deep; Key==from; Key==size; Key==get_data; Key==get_total->
    search_spec(Rest, Fields, Spec#{Key => Value});

search_spec([{sort, List}|Rest], Fields, Spec) ->
    case add_sort(List, Fields, []) of
        {ok, Sort} ->
            search_spec(Rest, Fields, Spec#{sort=>Sort});
        {error, Error} ->
            {error, Error}
    end;

search_spec([{filter, Filters}|Rest], Fields, Spec) ->
    case add_filter(Filters, Fields, #{}) of
        {ok, Filters2} ->
            search_spec(Rest, Fields, Spec#{filter=>Filters2});
        {error, Error} ->
            {error, Error}
    end.


add_filter([], _Fields, Acc) ->
    {ok, Acc};

add_filter([#{field:=Field}=Filter|Rest], Fields, Acc) ->
    Bool = maps:get(bool, Filter, 'and'),
    Base = maps:get(Bool, Acc, []),
    case to_bin(Field) of
        <<"data.", _/binary>> ->
            Filter2 = make_filter(Filter),
            add_filter(Rest, Fields, Acc#{Bool => [Filter2|Base]});
        <<"metadata.", _/binary>> ->
            Filter2 = make_filter(Filter),
            add_filter(Rest, Fields, Acc#{Bool => [Filter2|Base]});
        Field2 ->
            ValidFields = [
                <<"date">>, <<"app">>, <<"group">>, <<"resource">>, <<"type">>,
                <<"target">>, <<"level">>, <<"uid">>, <<"node">>
            ],
            case lists:member(Field2, ValidFields) of
                true ->
                    Filter2 = make_filter(Filter),
                    add_filter(Rest, Fields, Acc#{Bool => [Filter2|Base]});
                false ->
                    {error, {field_invalid, Field}}
            end
    end.

make_filter(Filter) ->
    maps:with([field, op, type, value], Filter).


%% @private
add_sort([], _Fields, Acc) ->
    {ok, lists:reverse(Acc)};

add_sort([Field|Rest], Fields, Acc) ->
    case binary:split(to_bin(Field), <<":">>) of
        [<<"asc">>, Field2] ->
            add_sort(Field2, asc, Rest, Fields, Acc);
        [<<"desc">>, Field2] ->
            add_sort(Field2, desc, Rest, Fields, Acc);
        [Field2] ->
            add_sort(Field2, asc, Rest, Fields, Acc)
    end.


%% @private
add_sort(Field, Order, Rest, Fields, Acc) ->
    add_sort(Rest, Fields, [#{field=>Field, order=>Order}|Acc]).

%%    case maps:find(Field, Fields) of
%%        {ok, {{op, _Op}, Field2}} when is_binary(Field2) ->
%%            add_sort(Rest, Fields, [#{field=>Field2, order=>Order}|Acc]);
%%        {ok, {{op, _Op, _OpValue}, Field2}} when is_binary(Field2) ->
%%            add_sort(Rest, Fields, [#{field=>Field2, order=>Order}|Acc]);
%%        {ok, Field2} when is_binary(Field2)->
%%            add_sort(Rest, Fields, [#{field=>Field2, order=>Order}|Acc]);
%%        _ ->
%%            {error, {field_invalid, Field}}
%%    end.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).
