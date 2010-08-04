#include <curl/curl.h>

#include "couch_lucene.h"

#ifdef _MSC_VER
    #include <direct.h>
    #define RMDIR(d) _rmdir(d)
#elif unix
    #include <unistd.h>
    #include <sys/stat.h>
    #include <sys/types.h>
    #define RMDIR(d) rmdir(d)
#endif

using namespace lucene::index;
using namespace lucene::analysis;
using namespace lucene::util;
using namespace lucene::store;
using namespace lucene::queryParser;
using namespace lucene::document;
using namespace lucene::search;

static const string COUCH_HOST  = "http://localhost:5984/";

static const wstring WSEQ_NUM_FIELD  = L"_seq_num";
static const wstring WID_FIELD  = L"_id";
static const wstring WSEQ_NUM_PREFIX  = L"seq";

/****************************************************
* Static Functions
****************************************************/
static char errorBuffer[CURL_ERROR_SIZE];  

struct write_result {
	std::string buffer;
};

size_t curl_write( void *ptr, size_t size, size_t nmemb, void *stream)
{
	struct write_result *result = (struct write_result *)stream;
	result->buffer.append((char*)ptr, size*nmemb);
	return size * nmemb;
}

char wide_to_narrow(TCHAR w)
{
    // simple typecast
    // works because UNICODE incorporates ASCII into itself
    return char(w);
}

/* The class of the global object. */
static JSClass global_class = {
    "global", JSCLASS_GLOBAL_FLAGS,
    JS_PropertyStub, JS_PropertyStub, JS_PropertyStub, JS_PropertyStub,
    JS_EnumerateStub, JS_ResolveStub, JS_ConvertStub, JS_FinalizeStub,
    JSCLASS_NO_OPTIONAL_MEMBERS
};

/* The error reporter callback. */
void reportError(JSContext *cx, const char *message, JSErrorReport *report)
{
    fprintf(stderr, "%s:%u:%s\n",
            report->filename ? report->filename : "<no filename>",
            (unsigned int) report->lineno,
            message);
}


/***************************************************
*
* CouchLucene
*
*****************************************************/
CouchLucene::CouchLucene()
{
}

CouchLucene::CouchLucene(string* dir)
{
	indexDir = dir;
}

CouchLucene::~CouchLucene()
{
}


void CouchLucene::handle_request(const string &request)
{
	// default just echo the request back to the caller

	// request is a JSON document so parse
	Json::Value root;   // contains the root value after parsing.
	Json::Reader reader;
    istringstream requeststream(request); 
	bool parsingSuccessful = reader.parse(requeststream, root);

    Json::Value response;
    
	if (parsingSuccessful)
	{
		// echo request back to caller
		// {"code": 200, "json": request, "headers": {}})
		response["code"] = 200;
		response["json"] = root;
	}
	else
	{
        response["code"] = 400;
        response["body"] = reader.getFormatedErrorMessages();
        
	}

    Json::FastWriter writer;
    // Make a new JSON document.
    std::string output = writer.write( response );
    cout << output.c_str() << endl;
}
void CouchLucene::write_error(const string &err, int code)
{
	Json::Value response;
    response["code"] = code;
    response["body"] = err;
    Json::FastWriter writer;
    // Make a new JSON document.
    std::string output = writer.write( response );
    cout << output.c_str() << endl;
}

/***************************************************
*
* CouchLuceneUpdater
*
*****************************************************/
CouchLuceneUpdater::CouchLuceneUpdater(string* dir, int count)
{
	indexDir = dir;
	optimize_count = count;

	// initialise the js engine
	 /* Create a JS runtime. */
    rt = JS_NewRuntime(8L * 1024L * 1024L);
    if (rt == NULL)
   	  write_error("Error creating JS Runtime", 500);

    /* Create a context. */
    cx = JS_NewContext(rt, 8192);
    if (cx == NULL)
		write_error("Error creating JS Context", 500);

    JS_SetErrorReporter(cx, reportError);
	
	JS_SetOptions(cx, JSOPTION_VAROBJFIX);

    /* Create the global object. */
    global = JS_NewObject(cx, &global_class, NULL, NULL);
    if (global == NULL)
		write_error("Error creating JS global object", 500);

    /* Populate the global object with the standard globals,
       like Object and Array. */
    if (!JS_InitStandardClasses(cx, global))
		write_error("Error initialising JS standard classes", 500);

}

CouchLuceneUpdater::~CouchLuceneUpdater()
{
    JS_DestroyContext(cx);
    JS_DestroyRuntime(rt);
    JS_ShutDown();
}

void CouchLuceneUpdater::handle_request(const string &request)
{
	// { "type" : "updated", "db" : "test" } }
	// or {"type": "deleted", "db", "test" } }
	Json::Value root;   // contains the root value after parsing.
	Json::Reader reader;
	istringstream requeststream(request); 
	bool parsingSuccessful = reader.parse(requeststream, root);
	std::stringstream tmpStr;

	if (parsingSuccessful)
	{

		// get the dbName
		string dbName = root["db"].asString();
		string type = root["type"].asString();
        
		if (type.compare("updated") == 0)
		{
			// find the counter for this db
			map<string, int>::iterator itr = updateCntrMap.find(dbName);
			if (itr != updateCntrMap.end())
			{
				int count = itr->second;

				if (++count == optimize_count)
				{
					optimize(dbName);
					count = 0;
				}

				updateCntrMap[dbName] = count;
			}
			else
				updateCntrMap[dbName] = 0; // first time through, count from zero

			update_index(dbName);

		}
		else if (type.compare("deleted") == 0)
		{
			// delete clucene index
			if ((this->indexDir->c_str())[indexDir->length() - 1] == '/')
				tmpStr << indexDir->c_str() << dbName.c_str();
			else
				tmpStr << indexDir->c_str() << "/" << dbName.c_str();

			const std::string& tmp = tmpStr.str();
			const char* target = tmp.c_str();

			if (IndexReader::indexExists(target) != false)
			{
				WhitespaceAnalyzer an;
				IndexWriter* writer = _CLNEW IndexWriter(target, &an, true);

				Directory* dir = writer->getDirectory();
				vector<string> allFiles;
			    dir->list(allFiles);

				for (vector<string>::iterator it = allFiles.begin(); it!=allFiles.end(); ++it) {
					dir->deleteFile(it->c_str(), false);
				}

				writer->close();
				_CLDELETE(writer);

				// remove the directory
				RMDIR(target);
				
				// remove updateCntrMap entry for this db
				updateCntrMap.erase(dbName);

			}

		}
		else
		{
			write_error("message type not recognised", 400);
		}
	}
	else
	{
		write_error(reader.getFormatedErrorMessages(), 400);
	}
}

void CouchLuceneUpdater::optimize(string index)
{
	IndexReader* reader = NULL;
	WhitespaceAnalyzer an;
	std::stringstream tmpStr;

	if ((this->indexDir->c_str())[indexDir->length() - 1] == '/')
		tmpStr << indexDir->c_str() << index.c_str();
	else
		tmpStr << indexDir->c_str() << "/" << index.c_str();

	const std::string& tmp = tmpStr.str();
	const char* target = tmp.c_str();

	if (IndexReader::indexExists(target) == false)
	{
		// create a new index
		IndexWriter* writer = _CLNEW IndexWriter(target, &an, true);
		writer->close();
		_CLDELETE(writer);
	}
	else
	{
		// optimise the index
		IndexWriter *writer = _CLNEW IndexWriter(target, &an, false);
		writer->optimize();
		writer->close();
		_CLDELETE(writer);

	}

}

void CouchLuceneUpdater::update_index(string index)
{
	IndexReader* reader = NULL;
	WhitespaceAnalyzer an;
	std::stringstream tmpStr;
	std::wostringstream wTmpStr;
	
	if ((this->indexDir->c_str())[indexDir->length() - 1] == '/')
		tmpStr << indexDir->c_str() << index.c_str();
	else
		tmpStr << indexDir->c_str() << "/" << index.c_str();

	const std::string& tmp = tmpStr.str();
	const char* target = tmp.c_str();

	if (IndexReader::indexExists(target) == false)
	{
		// create a new index
		IndexWriter* wrtr = _CLNEW IndexWriter(target, &an, true);
		wrtr->close();
		_CLDELETE(wrtr);
	}

	// read the current seq_num
	reader = IndexReader::open(target);
	IndexSearcher searcher(reader);

	QueryParser* qp = _CLNEW QueryParser(WSEQ_NUM_FIELD.c_str(), &an);
		
	// clear
	wTmpStr.clear();
	wTmpStr.str();
	wTmpStr << WSEQ_NUM_PREFIX.c_str() << "*";	

	const std::wstring& wtmp = wTmpStr.str();
	const TCHAR* wquery_string = wtmp.c_str();
	Query* q = qp->parse(wquery_string);
	Hits* h = searcher.search(q);

    string seq_num;

	if (h->length() > 0)
	{
		Document* doc = &h->doc(0);
		// parse seq number
		// clear
		wTmpStr.clear();
		wTmpStr.str(std::wstring());
		
		wTmpStr << doc->getField(WSEQ_NUM_FIELD.c_str())->stringValue();
        wstring wResult = wTmpStr.str();
		wResult.erase(0, WSEQ_NUM_PREFIX.length());
		
        seq_num = string(wResult.length(), ' ');
        transform(wResult.begin(), wResult.end(), seq_num.begin(), wide_to_narrow);

		// delete existing config doc as we are going to add it again with an updated number
		Term* t1 = _CLNEW Term(WSEQ_NUM_FIELD.c_str(), doc->getField(WSEQ_NUM_FIELD.c_str())->stringValue());		
		reader->deleteDocuments(t1);

		reader->flush();

		_CLDECDELETE(t1);
	}
	else
	{
		seq_num = "0";
	}
	
	_CLDELETE(h);
	_CLDELETE(q);
	_CLLDELETE(qp);

	searcher.close();
	reader->close();
	_CLDELETE(reader);

	// index files
	// fetch changes from couchdb
	wostringstream wseq;
	wseq << seq_num.c_str();

	long last_seq_num = addChanges(target, seq_num.c_str(), &index);

	wostringstream wstream;
	wstream << WSEQ_NUM_PREFIX.c_str() << last_seq_num;
	const wstring wlast_seq_num = wstream.str();

	// add a new couch config doc	
	Document config;
	config.add(*_CLNEW Field(WSEQ_NUM_FIELD.c_str(), wlast_seq_num.c_str(), Field::STORE_YES | Field::INDEX_UNTOKENIZED));
	write_doc(target, &config, &an, false);
}

void CouchLuceneUpdater::write_doc(const char* target, Document* doc, Analyzer* an, bool create)
{
	IndexWriter *writer = _CLNEW IndexWriter(target , an, create);
	writer->addDocument(doc);
	writer->flush();
	writer->close();
	_CLDELETE(writer);
}

long CouchLuceneUpdater::addChanges(const char* target, const char* since_seq_num, const string* dbName)
{
  CURL *curl_handle;
  CURLcode res;
  stringstream url;
  wostringstream query;
  WhitespaceAnalyzer an;
  IndexReader* reader;
  long last_seq_num = 0;

  curl_handle = curl_easy_init();
  if (curl_handle) 
  {
	  // clear global response buffer
	  struct write_result changes_result;

	  url << COUCH_HOST << dbName->c_str() << "/" << "_changes?since=" << since_seq_num << "&include_docs=true";
	  const std::string& tmp = url.str();
	  const char* url_string = tmp.c_str();

	  //curl_easy_setopt(curl_handle, CURLOPT_ERRORBUFFER, errorBuffer);
	  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_write);
	  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &changes_result);
	  curl_easy_setopt(curl_handle, CURLOPT_URL, url_string);
	  res = curl_easy_perform(curl_handle);

	  // parse the incoming JSON
	  istringstream resultstream(changes_result.buffer);

	  Json::Value root;
	  Json::Reader rdr;
	  bool parsingSuccessful = rdr.parse( resultstream, root );

	  if (parsingSuccessful)
	  {
		
		// query by doc id
		QueryParser* qp = _CLNEW QueryParser(WID_FIELD.c_str(), &an);
		
		const Json::Value arrayChanges = root["results"];

		// iterate over changes
		for ( int index = 0; index < arrayChanges.size(); ++index )  
		{
			const Json::Value& objChange = arrayChanges[index];

			// get the sequence number for each document
			// get the id for each doc
			const string& id = objChange["id"].asString();

			// get the doc
			Json::Value jsonDoc = objChange["doc"];
			// write json doc to string
			Json::FastWriter wrtr;
			string docStr = wrtr.write(jsonDoc);

			// don't index design documents
			if (id.find("_design") == string::npos)
			{
				wostringstream wStr;
				wStr << id.c_str();
				const wstring wtmp = wStr.str();

				// you can write (at the moment) a design document with a null id
				if (wtmp.length() > 0)
				{
				
					const TCHAR* wId_string = wtmp.c_str();

					// read the current seq_num
					reader = IndexReader::open(target);
					IndexSearcher searcher(reader);

					Query* q = qp->parse(wId_string);
					Hits* h = searcher.search(q);

					if (h->length() > 0)
					{
						// remove existing document
						Document doc = h->doc(0);
						Term* t1 = _CLNEW Term(WID_FIELD.c_str(), wId_string);
						reader->deleteDocuments(t1);
						reader->flush();
						_CLDECDELETE(t1);
					}

					searcher.close();
					reader->close();

					_CLDELETE(reader);
					_CLDELETE(h);
					_CLDELETE(q);
					
					// if objChanges is not marked as deleted then it add it back
					Json::Value deletedValue;
					deletedValue = objChange.get("deleted", false);

					if (deletedValue.asBool() == false)
					{
						// not marked as deleted
						// so add the document back	
						Document newdoc;
						wostringstream widstream;
						widstream << id.c_str();
						const wstring wid = widstream.str();

						newdoc.add(*_CLNEW Field(WID_FIELD.c_str(), wId_string, 
							Field::STORE_YES));
						
						// get the index functions and names for this db
						// dbName, (designId, (term, {defaults, script}))
						// map<string, map <string, FtiDefn > >  , designId, (term, defaults)
						map<string, map <string, string > > queryMap = ftiMap[*dbName];

						for (map<string, map <string, string > >::iterator i = queryMap.begin(); i != queryMap.end(); i++)
						{
							// i->first; designId
							// i->second; term, defaults
							map<string, string> termMap = i->second;

							for (map<string, string>::iterator iFti = termMap.begin(); iFti != termMap.end();  iFti++)
							{	
								jsval jsresult;
								char *filename = NULL;
								uintN lineno = 0;
								JSBool ok;
					
								string term = iFti->first;
								string defaults = termMap[term];

								// put the current doc in scope
								ostringstream js;
								js << "var doc = ";
								js << docStr << ";" << endl;
								
								// term is the prototype so we can the function
								js << term.c_str() << "(doc);" << endl;

								const std::string tmp = js.str();
								const char* js_str = tmp.c_str();
								
								ok = JS_EvaluateScript(cx, global, js_str, strlen(js_str),
													filename, lineno, &jsresult);

								if (ok)
								{
									// result is either a JSON structure
									// {"value": _, "type": _, "field": _}
									// just a string
									wostringstream wv;

									if (JSVAL_IS_OBJECT(jsresult) && !JSVAL_IS_NULL(jsresult))
									{
										jsval val;
										JSObject* obj = (JSObject*)jsresult;
										JS_GetProperty(cx, obj, "value", &val);
										wv << JS_GetStringBytes(JS_ValueToString(cx, val));
									}
									else
									{
										wv << JS_GetStringBytes(JS_ValueToString(cx, jsresult));
									}

									const std::wstring& wtmp = wv.str();

									if (wtmp.compare(L"undefined") != 0)
									{
										const TCHAR* wval = wtmp.c_str();

										wostringstream wTermStream;
										wTermStream << term.c_str();
										const std::wstring& wTerm = wTermStream.str();

										// add term and value to lucene index
										newdoc.add(*_CLNEW Field(wTerm.c_str(), wval, Field::STORE_YES | Field::INDEX_TOKENIZED));							
									}
								}
							}
						}
						
						JS_MaybeGC(cx);

						write_doc(target, &newdoc, &an, false);
					}
				}
			}
			else
			{
				// we have a design document, parse for FTI functions
				Json::Value jsonDoc = objChange["doc"];
				Json::Value fti;
				fti = jsonDoc.get("fulltext", 0u);

				if (fti.size() > 0)
				{
					Json::Value ftiObject = jsonDoc["fulltext"];
					parseFTI(dbName, &id, ftiObject);
				}

			}

		} // end for loop


		_CLLDELETE(qp);

		// get the last seq num
		last_seq_num = (long) root["last_seq"].asUInt();
	  }
	  else
	  {
		write_error(rdr.getFormatedErrorMessages(), 400);
	  }

	  curl_easy_cleanup(curl_handle);
  }

  return last_seq_num;
}

void CouchLuceneUpdater::parseFTI(const string* dbName, const string* designDocName, Json::Value& ftiObject)
{
	// iterate over members of fulltext
	for (int idxFti = 0; idxFti < ftiObject.size(); ++idxFti ) 
	{

		string name = ftiObject.getMemberNames()[idxFti]; // this is part of the query term

		// remove _design prefix
		string term = (*dbName + "_" + name);

		Json::Value member = ftiObject[name];

		Field::Store store = Field::STORE_YES;
		JSScript* script = NULL;

		// get the defaults and the script
		Json::Value defaults;
		defaults = member.get("defaults", 0u);
		
		if (defaults.size() > 0)
		{
			// "defaults": {"store": "yes"}
			// look for store
			Json::Value store;
			store = defaults.get("store", 0u);
			if (store.isString() > 0)
			{
				if (store.asString().compare("no") == 0)
					store == Field::STORE_NO;
			}
		}


	    // get index, the value is the fti script
		Json::Value fulltext = 0u;
		fulltext = member.get("index", 0u);

		if (fulltext.isString())
		{
			string jsStr = fulltext.asString();
			const char* js = jsStr.c_str();

			jsval result;
			char *filename = NULL;
			uintN lineno = 0;

			ostringstream js_stream;
			// remove _design from ddoc
			js_stream << "var " << term.c_str() << "=" << js << ";";
			//js_stream << "var " << name.c_str() << "=" << js << ";";
			const std::string& tmp = js_stream.str();
			const char* js_string = tmp.c_str();
			JSBool ok;

			ok = JS_EvaluateScript(cx, global, js_string , strlen(js_string),
											filename, lineno, &result);
			if (!ok)
			{
				wostringstream w;
				w << js_string;

				MessageBox(NULL, w.str().c_str(), _T("FTI"), NULL);
				write_error(js_string, 500);
			}
		}

		// fti member map looks like  dbName, (designId, (term name, defaults)
		ftiMap[*dbName][*designDocName][term.c_str()] = store;
	}
}

void CouchLuceneUpdater::get_design_docs()
{
	CURL *curl_handle;
	CURLcode res;
	ostringstream url;

	// clear global response buffer
	struct write_result write_result;

	url << COUCH_HOST << "_all_dbs";

	curl_handle = curl_easy_init();
	if (curl_handle) 
	{
		const std::string& tmp = url.str();
		const char* url_str = tmp.c_str();

		curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_write);
		curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &write_result);
		curl_easy_setopt(curl_handle, CURLOPT_URL, url_str);
		res = curl_easy_perform(curl_handle);

		// parse the response
        istringstream resultstream(write_result.buffer);

		Json::Value root;
		Json::Reader reader;
		bool parsingSuccessful = reader.parse( resultstream, root );                
		
		if (parsingSuccessful)
		{
			// root is an array
			for (int index = 0; index < root.size(); ++index)
			{
				const string db = root[index].asString();

				// for each db get all design docs and if they have an FTI component then add to the fti map
				// e.g. http://localhost:5984/db/_all_docs?startkey=%22_design%22&endkey=%22_design0%22&include_docs=true
				url.str("");
				
				struct write_result design_result;
				url << COUCH_HOST << db.c_str() << "/_all_docs?startkey=%22_design%22&endkey=%22_design0%22&include_docs=true";	

				const std::string& tmpDesign = url.str();
				const char* url_design_str = tmpDesign.c_str();

				//curl_easy_setopt(curl_handle, CURLOPT_ERRORBUFFER, errorBuffer);
				curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_write);	
				curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &design_result);
				curl_easy_setopt(curl_handle, CURLOPT_URL, url_design_str);
				res = curl_easy_perform(curl_handle);

				if (res == CURLE_OK)
				{
					Json::Value designRoot;
					istringstream designstream(design_result.buffer);
					
					Json::Reader reader;
					bool designParsingSuccessful = reader.parse( designstream, designRoot ); 
					if (designParsingSuccessful)
					{

						const Json::Value rows = designRoot["rows"];
						for (int rowIdx = 0; rowIdx < rows.size(); ++rowIdx)
						{
							// see if we have a full text field
							const string designId = rows[rowIdx]["id"].asString();
							
							Json::Value ftiObject;
							ftiObject = rows[rowIdx]["doc"].get("fulltext", 0u);
							
							if (ftiObject.size() > 0)
							{			
								parseFTI(&db, &designId, ftiObject);
							}
						}
					}
					else
					{
						// parsing failed
						write_error(reader.getFormatedErrorMessages(), 400);
					}
				}
				else
				{
					write_error("error getting design docs", 500);
				}
			}
		}
		else
		{

			// parsing failed
			write_error(reader.getFormatedErrorMessages(), 400);
		}

		curl_easy_cleanup(curl_handle);
	}
}

/***************************************************
*
* CouchLuceneQuery
*
*****************************************************/
CouchLuceneQuery::CouchLuceneQuery(string* dir)
{
	indexDir = dir;
}

CouchLuceneQuery::~CouchLuceneQuery()
{

}
void CouchLuceneQuery::get_doc(const char* db, const char* id, string& result)
{
	CURL *curl_handle;
	CURLcode res;
	ostringstream url;

	// clear global response buffer
	struct write_result write_result;

	url << COUCH_HOST << db << "/" << id;

	curl_handle = curl_easy_init();
	if (curl_handle) 
	{
		const std::string& tmp = url.str();
		const char* url_str = tmp.c_str();

		curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_write);
		curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &write_result);
		curl_easy_setopt(curl_handle, CURLOPT_URL, url_str);
		res = curl_easy_perform(curl_handle);

		// parse the response
        //istringstream resultstream(write_result.buffer);
		curl_easy_cleanup(curl_handle);
		result = string(write_result.buffer);
	}
}

void CouchLuceneQuery::get_bulk_docs(const char* db, const char* json_request, string& result)
{
	CURL *curl_handle;
	CURLcode res;
	ostringstream url;

	// clear global response buffer
	struct write_result write_result;

	url << COUCH_HOST << db << "/" << "_all_docs?include_docs=true";

	curl_handle = curl_easy_init();
	if (curl_handle) 
	{
		const std::string& tmp = url.str();
		const char* url_str = tmp.c_str();

		curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_write);
		curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &write_result);
		curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
		curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, json_request);
		curl_easy_setopt(curl_handle, CURLOPT_URL, url_str);
		res = curl_easy_perform(curl_handle);

		// parse the response
        //istringstream resultstream(write_result.buffer);
		curl_easy_cleanup(curl_handle);
		result = string(write_result.buffer);
	}
}

void CouchLuceneQuery::handle_request(const string &request)
{
/*
	{
		"info":{
			"db_name":"test",
			"doc_count":1,
			"doc_del_count":1,
			"update_seq":7,
			"purge_seq":0,
			"compact_running":false,
			"disk_size":28761,
			"instance_start_time":"1275666414427000",
			"disk_format_version":5
			},
		"id":null,
		"method":"GET",
		"path":["test","_fti"],
		"query":{"q":"request"},
		"headers":{"Accept":"*\\/*",
		"Host":"localhost:5984",
		"User-Agent":"curl/7.17.1 (i586-pc-mingw32msvc) libcurl/7.17.1 OpenSSL/0.9.7e zlib/1.2.3"},
		"body":"undefined",
		"peer":"127.0.0.1",
		"form":{},
		"cookie":{},
		"userCtx":{"db":"test","name":null,"roles":["_admin"]}}
*/

	WhitespaceAnalyzer analyzer;

	// parse the incoming json
	Json::Value root;
	Json::Reader reader;
	istringstream requeststream(request); 
	bool parsingSuccessful = reader.parse( requeststream, root );

	if (parsingSuccessful)
	{

		const Json::Value arrPath = root["path"];
		string db = arrPath[0u].asString();

		string ftiHandler = arrPath[1u].asString();

		if (arrPath.size() > 2)
		{
			// term name is included
			string term = arrPath[2u].asString();

			// parse the query string
			Json::Value queryObject = root["query"];

			// is there a query
			Json::Value query = 0u;
			query = queryObject.get("q", 0u);
			
			Json::Value include_docs = 0u;
			include_docs = queryObject.get("include_docs", 0u);

			Json::Value skip = 0;
			skip = queryObject.get("skip", 0);

			Json::Value limit = 0u;
			limit = queryObject.get("limit", 0u);

			wostringstream qstream;
			qstream << query.asString().c_str();

			if (!query.isNull())
			{

				const string queryString = queryObject["q"].asString();

				// create index reader and perform lucene query
				stringstream tmpStr;
				
				if ((this->indexDir->c_str())[indexDir->length() - 1] == '/')
					tmpStr << indexDir->c_str() << db.c_str();
				else
					tmpStr << indexDir->c_str() << "/" << db.c_str();


				const std::string& tmpTgt = tmpStr.str();
				const char* target = tmpTgt.c_str();

				IndexReader* reader = IndexReader::open(target);
				IndexSearcher s(reader);
				
				wostringstream wFld_stream;
				wFld_stream << (db + "_" + term).c_str();
				const wstring& wFldTmp_string = wFld_stream.str();
				const wchar_t* wfld_string = wFldTmp_string.c_str();

				wostringstream wQueryStream;

				wQueryStream << queryString.c_str();
				const std::wstring& tmp = wQueryStream.str();
				const TCHAR* wquery_string = tmp.c_str();

				Query* query = QueryParser::parse(wquery_string, wfld_string, &analyzer);	
				Hits* h = s.search(query);

				// create json response
				Json::Value objResult;
				Json::Value rows;

				// create a json array of all document ids
				Json::Value docIds;

				if (h->length() > 0)
				{

					int start = skip.asInt();

					int end = h->length();


					if (limit.asInt() > 0)
						end = limit.asInt();

					wstringstream wresultstream;
					wresultstream << end;

					for (int32_t i = start; i < end;i++ ){

						Document* doc = &h->doc(i);
						Json::Value obj;
						
						Field* fld = doc->getField(WID_FIELD.c_str());
						if (fld != NULL)
						{

							// get the document id
							wstring wval = wstring(fld->stringValue());
							string id = string(wval.length(), ' ');
							transform(wval.begin(), wval.end(), id.begin(), wide_to_narrow);
							
							float score = h->score(i);

							obj["id"] = id;
							obj["score"] = score;
							
							wostringstream w;
							w << include_docs.asBool();

							if ((include_docs.isNull() == false) && (include_docs.asBool() == true))
							{
								// add the doc
								// string result_doc;
								//get_doc(db.c_str(), id.c_str(), result_doc);
								//obj["doc"] = result_doc;
								// store doc id for bulk fetch later
								docIds[i] = id.c_str();
							}

						}

						rows.append(obj);
					}
				}

				s.close();
				reader->close();
				_CLDELETE(h);
				_CLDELETE(query);
				_CLDELETE(reader);

				if (rows.size() == 0)
				{
					cout << "{\"code\": 200, \"json\": {\"rows\" : []}}" << endl;
				}
				else
				{
					// do we need to bulk fetch the documents for inclusion
					if (docIds.size() > 0)
					{
						// make a bulk document request
						// format is {"keys":["bar","baz"]}
						Json::Value keys;
						keys["keys"] = docIds;
						Json::FastWriter wrtr;
						string json_request = wrtr.write(keys);

						string result_doc;
						get_bulk_docs(db.c_str(), json_request.c_str(), result_doc);

						// parse the result
						Json::Reader rdr;
						Json::Value resultRoot;
						istringstream resultstream(result_doc); 
						bool success = rdr.parse(resultstream, resultRoot);
						if (success)
						{
							for (int i = 0; i < rows.size(); i++)
							{
								rows[i]["doc"] = resultRoot["rows"][i]["doc"];
							}
						}
						
					}

					objResult["code"] = 200;
					objResult["json"]["rows"] = rows;

					// serialize result to cout
					Json::FastWriter writer;
					// Make a new JSON document for the configuration. Preserve original comments.
					string output = writer.write(objResult);
					cout << output.c_str() << endl;
				}
			}
			else
			{
				// no query object
				cout << "{\"code\" : 400, \"body\":\"query parameter q required\"}" << endl;
			}
		}
		else
			cout << "{\"code\" : 400, \"body\":\"query term context required\"}" << endl;

	}
	else
	{
		write_error(reader.getFormatedErrorMessages(), 400);
	}
}


