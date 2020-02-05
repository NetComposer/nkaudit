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
-module(nkserver_audit_search).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([parse_spec/2]).
-export_type([spec/0, opts/0, filter/0, sort_spec/0]).

-include("nkserver_audit.hrl").



%% ==================================================================
%% Types
%% ===================================================================


-type spec() ::
#{
    namespace => nkserver_audit:namespace(),
    deep => boolean(),
    from => pos_integer(),
    size => pos_integer(),
    get_total => boolean(),
    filter => filter(),
    sort => [sort_spec()],
    get_data => boolean()
}.


-type filter() ::
#{
    'and' => [filter_spec()],
    'or' => [filter_spec()],
    'not' => [filter_spec()]
}.


-type field_name() :: binary().


% Field types are used to cast the correct value from JSON
% By default, they will be extracted as strings (so they will be sorted incorrectly)
% For JSON fields, the type 'object' can be used for @> operator
% for example, for field spec.phone.phone => array, the query
%   #{field => "spec.phone.phone", eq=>"123} generates data->'spec'->'phone' @> '[{"phone": "123456"}]'
% For 'range' queries, a value like min|max is expected (will find >= mind AND <= max)
% Use ! prefix to make strict: !min|!max is expected (will find > mind AND < max)

-type field_type() :: string | boolean | integer | object | array.


-type filter_spec() ::
#{
    field => field_name(),
    type => field_type(),
    op => filter_op(),
    value => value() | [value()]
}.

-type filter_op() ::
    eq | ne | lt | lte | gt | gte | values | exists | range | prefix.


-type value() :: string() | binary() | integer() | boolean().


-type sort_spec() ::
#{
    field => field_name(),      % '.' used to separate levels in JSON
    type => field_type(),
    order => asc | desc
}.


-type opts() ::
    #{}.


%% ===================================================================
%% Standard search
%% ===================================================================


%% @doc
-spec parse_spec(spec(), opts()) ->
    {ok, spec()} | {error, term()}.

parse_spec(SearchSpec, _Opts) ->
    Syntax = search_spec_syntax(),
    case nklib_syntax:parse(SearchSpec, Syntax) of
        {ok, Parsed, []} ->
            case check_filters(Parsed) of
                {ok, Parsed2} ->
                    check_sort(Parsed2);
                {error, Error} ->
                    {error, Error}
            end;
        {ok, _, [Field|_]} ->
            {error, {field_unknown, Field}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
search_spec_syntax() ->
    #{
        from => pos_integer,
        size => pos_integer,
        namespace => binary,
        deep => boolean,
        get_total => boolean,
        filter => #{
            'and' => {list, search_spec_syntax_filter()},
            'or' => {list, search_spec_syntax_filter()},
            'not' => {list, search_spec_syntax_filter()}
        },
        sort => {list, search_spec_syntax_sort()},
        get_data => boolean,
        get_metadata => boolean,
        '__defaults' => #{namespace => <<>>}
    }.


%% @private
search_spec_syntax_filter() ->
    #{
        field => binary,
        type => {atom, [string, integer, boolean, object]},
        op => {atom, [eq, ne, lt, lte, gt, gte, values, exists, prefix, range, ignore]},
        value => any,
        '__mandatory' => [field, value]
    }.


%% @private
search_spec_syntax_sort() ->
    #{
        field => binary,
        type => {atom, [string, integer, boolean]},
        order => {atom, [asc, desc]},
        '__mandatory' => [field],
        '__defaults' => #{order => asc}
    }.


%% @private
check_filters(#{filter:=Filter}=Spec) ->
    case check_filters(['and', 'or', 'not'], Filter) of
        {ok, Filter2} ->
            {ok, Spec#{filter:=Filter2}};
        {error, Error} ->
            {error, Error}
    end;

check_filters(Spec) ->
    {ok, Spec}.


%% @private
check_filters([], Filter) ->
    {ok, Filter};

check_filters([Type|Rest], Filter) ->
    case maps:find(Type, Filter) of
        {ok, FilterList} ->
            case check_field_filter(FilterList, []) of
                {ok, FilterList2} ->
                    check_filters(Rest, Filter#{Type:=FilterList2});
                {error, Error} ->
                    {error, Error}
            end;
        error ->
            check_filters(Rest, Filter)
    end.


%% @private
%% Checks the filter in the valid list
check_field_filter([], Acc) ->
    {ok, lists:reverse(Acc)};

check_field_filter([#{field:=Field}=Filter|Rest], Acc) ->
    case field_name(Field) of
        ok ->
            Filter2 = filter_op(Filter),
            Filter3 = field_type(Filter2),
            check_field_filter(Rest, [Filter3|Acc]);
        error ->
            {error, {field_invalid, Field}}
    end.


%% @private
check_sort(#{sort:=Sort}=Spec) ->
    case check_field_sort(Sort, []) of
        {ok, Sort2} ->
            {ok, Spec#{sort:=Sort2}};
        {error, Error} ->
            {error, Error}
    end;

check_sort(Spec) ->
    {ok, Spec}.


%% @private
%% Checks the filter in the valid list
check_field_sort([], Acc) ->
    {ok, lists:reverse(Acc)};

check_field_sort([#{field:=Field}=Sort|Rest], Acc) ->
    case field_name(Field) of
        ok ->
            Sort2 = field_type(Sort),
            check_field_sort(Rest, [Sort2|Acc]);
        error ->
            {error, {field_invalid, Field}}
    end.


%% @private
field_name(<<"uid">>) -> ok;
field_name(<<"date">>) -> ok;
field_name(<<"app">>) -> ok;
field_name(<<"group">>) -> ok;
field_name(<<"type">>) -> ok;
field_name(<<"level">>) -> ok;
field_name(<<"trace">>) -> ok;
field_name(<<"id">>) -> ok;
field_name(<<"id2">>) -> ok;
field_name(<<"id3">>) -> ok;
field_name(<<"msg">>) -> ok;
field_name(<<"data">>) -> ok;
field_name(<<"data.", _/binary>>) -> ok.


%% @private
field_type(#{type:=_}=Filter) -> Filter;
field_type(#{field:=<<"level">>}=Filter) -> Filter#{type=>integer};
field_type(Filter) -> Filter#{type=>string}.


%% @private
filter_op(#{op:=_}=Filter) ->
    Filter;

filter_op(#{value:=Value}=Filter) ->
    {Op, Value2} = expand_op(Value),
    Filter#{op=>Op, value:=Value2}.



%% ===================================================================
%% Param-based search
%% ===================================================================



%% @private
expand_op(Value) ->
    case binary:split(to_bin(Value), <<":">>) of
        [Op, Value2] ->
            Op2 = case catch binary_to_existing_atom(Op, latin1) of
                eq -> eq;
                ne -> ne;
                gt -> gt;
                gte -> gte;
                lt -> lt;
                lte -> lte;
                prefix -> prefix;
                values -> values;
                range -> range;
                _ -> none
            end,
            case Op2 of
                values ->
                    {values, binary:split(Value2, <<"|">>, [global])};
                none ->
                    {eq, Value};
                _ ->
                    {Op2, Value2}
            end;
        [<<>>] ->
            {exists, true};
        [Value2] ->
            {eq, Value2}
    end.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

