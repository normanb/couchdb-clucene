-module(wikipedia).

-export([load/2, load/3]).

-define(BATCH_COUNT, 10).

load(FileName, Db) ->
  load(FileName, "http://localhost:5984", Db).
  
load(FileName, Host, Db) ->
  ibrowse:start(),
  Url = Host ++ "/" ++ Db,
  
  % check the db exists
  case ibrowse:send_req(Url, [], head) of
    {ok, "200", _, _} ->
       ok;
    _ ->
      % create the database
      ibrowse:send_req(Url, [], put),
      % add the free text function
      JsonFti = "{\"fulltext\": {\"by_text\": {\"defaults\": {\"store\": \"yes\"},\"index\": \"function(doc) {return {\\\"value\\\": doc.Text}}\"}}}",
      ibrowse:send_req(Url ++ "/_design/main", [{"Content-Type", "application/json"}], put,  JsonFti) 
  end,    
  
  % read the file
  {ok, Device} = file:open(FileName, read),
  {Counter, RemLines} = get_all_lines(Device, 0, [], Url),
  case RemLines of
    [] ->
      ok;
    _ ->
      update_couch(RemLines , Url),
      io:fwrite("Written ~p docs ~n", [Counter])
  end.
     

get_all_lines(Device, Counter, Acc, Url) when length(Acc) == ?BATCH_COUNT ->
  % bulk doc update of couchdb
  update_couch(Acc, Url),
  io:fwrite("Written ~p docs ~n", [Counter]),
  get_all_lines(Device, Counter, [], Url);

get_all_lines(Device, Counter, Acc, Url) ->
    case io:get_line(Device, "") of
        eof  -> 
            file:close(Device),
            {Counter, Acc};
        Line ->
            % replace " with '
            [Id, Title, Date, Text] = [ re:replace(X,"\"", "'", [{return,list}, global]) || X <- string:tokens(Line, "\t")],
            % create the JSON document and add to the accumulator
            Json = io_lib:format("{\"_id\": \"~s\", \"Title\": \"~s\", \"Date\": \"~s\", \"Text\": \"~s\"}", [Id, Title, Date, Text]),
            get_all_lines(Device, Counter + 1, [Json|Acc], Url)
    end.
    
update_couch(JsonRows, Url) ->
  [First| RemRows] = lists:reverse(JsonRows),
  % post a document like {"docs":[{"key":"baz","name":"bazzel"},{"key":"bar","name":"barry"}]} to _bulk_docs
  
  BodyStart = "{\"docs\":[" ++ First,
  
  Body = [BodyStart | [[","|R] || R <- RemRows]] ++ "]}",
  
  case ibrowse:send_req(Url ++ "/_bulk_docs", [{"Content-Type", "application/json"}], post,  Body) of
    {ok, "201", _, _} ->
      ok;
    _ ->
      erlang:error({updateError, "error updating couchdb"})
  end.

  
