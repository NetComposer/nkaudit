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

%% @doc Default callbacks for plugin definitions
-module(nkaudit_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_start/3]).

-include("nkaudit.hrl").
-include_lib("nkserver/include/nkserver.hrl").


%% ===================================================================
%% Plugin Callbacks
%% ===================================================================


%% @doc 
plugin_deps() ->
	[].


%% @doc
plugin_start(SrvId, _Config, _Service) ->
	lager:info("NkAUDIT starting sender (~s)", [SrvId]),
	Spec = #{
		id => SrvId,
		start => {nkaudit_sender, start_link, [SrvId]},
		restart => permanent,
		shutdown => 5000,
		type => worker,
		modules => [nkaudit_sender]
	},
	case nkserver_workers_sup:update_child2(SrvId, Spec, #{}) of
		{ok, _, _Pid} ->
			ok;
		{error, Error} ->
			{error, Error}
	end.
