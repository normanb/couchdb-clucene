#include <iostream>
#include <algorithm>
#include <vector>
#include <sstream>
#include <string>
#include <map>
#include <jsapi.h>
#include <json/json.h>
#include <CLucene.h>
#include <assert.h>

using namespace std;

class CouchLucene {
protected:
	string* indexDir;
	virtual void write_error(const string &error, int code);
public:
	CouchLucene();
	CouchLucene(string* dir);
	virtual ~CouchLucene();
	virtual void handle_request(const string &request);
};

class CouchLuceneQuery : public CouchLucene {
protected:
	void get_doc(const char* db, const char* id, string& result);
	void get_bulk_docs(const char* db, const char* json_request, string& result);
public:
    CouchLuceneQuery(string* dir);
	~CouchLuceneQuery();
	void handle_request(const string &request);
};

class CouchLuceneUpdater : public CouchLucene {
private:
	/* JS variables. */
    JSRuntime *rt;
    JSContext *cx;
    JSObject  *global;
	int optimize_count;
    map<string, int> updateCntrMap; // dbName, updateCntr
	map< string, map<string, map <string, string > > > ftiMap; // dbName, (designId, (term, defaults}))
	void parseFTI(const string* dbName, const string* designDocName, Json::Value& ftiObject);
protected:
	// return the last sequence number as the result
	void optimize(string index);
	void write_doc(const char* target, lucene::document::Document* doc, lucene::analysis::Analyzer* an, bool create);
	long addChanges(const char* target, const char* since_seq_num, const string* dbName);
public:
    CouchLuceneUpdater(string* dir, int count);
	~CouchLuceneUpdater();
	void handle_request(const string &request);
	void update_index(string index);
	void get_design_docs();
};


