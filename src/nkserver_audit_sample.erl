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
-module(nkserver_audit_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([insert/1, q1/1]).

insert(Srv) ->
    Audits = [
        #{app=>app1, group=>group1, level=>notice, data=>#{a=>1}},
        #{app=>app1, group=>group2, namespace=>"a.b", data=>#{b=>2}, metadata=>#{count=>2}},
        #{app=>app2, group=>group2, namespace=>"a.b", level=>notice, data=>#{c=><<"c">>}}
    ],
    nkserver_audit:store(Srv, Audits, #{}).


q1(Srv) ->
    S = #{
        get_total => true,
        deep =>true,
        get_data => true,
        namespace => b,
        filter => #{
            'and' => [
                #{field=>app, op=>values, value=>[app1, app2]},
                #{field=>date, op=>prefix, value=>"20"},
                #{field=>"data.b", type=>integer, value=>2}
            ]
        },
        sort => [#{field=>date}, #{field=>app}, #{field=>"data.c", type=>integer}]
    },
    nkserver_audit:search(Srv, S, #{}).