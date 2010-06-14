/**
* Simple wrapper around CLucene - http://clucene.sourceforge.net/
* Using Cajun - http://sourceforge.net/projects/cajun-jsonapi/
*/
#include <iostream>
#include <string>
#include <signal.h>
#include "couch_lucene.h"

using namespace std;

CouchLucene* couch = NULL;

void terminate(int param)
{
  //if (couch != NULL)
//	  delete couch;

  exit(1);
}


int main(int argc, const char * argv[])
{
	void (*prev_fn)(int);
	prev_fn = signal (SIGTERM,terminate);
	if (prev_fn==SIG_IGN) signal (SIGTERM,SIG_IGN);

	string line;
	string update = string("update");
	string indexDir;
	int mode = 0; // update mode get design docs

	if(argc != 3)
    {
		cerr << "incorrect number of arguments" << endl;
        cerr << "usage: " << argv[0] << " <index_directory>" << " mode" << endl;
        return 2;
    }
	else
    {
		indexDir	= string(argv[1]);

		// execute clucene storing index in argv[1]
		// mode is in argv[2]
		if (update.compare(argv[2]) == 0)
		{
			// update
			mode = 1; // flag that we need to get the design docs
			couch = new CouchLuceneUpdater(&indexDir);	
		}
		else
		{
			// query
			couch = new CouchLuceneQuery(&indexDir);
		}

		while (1)
		{		
			getline(cin, line);
            if (line.length() > 0)
			{
				if (mode == 1)
				{
					// on the first request get the design docs
					((CouchLuceneUpdater*)couch)->get_design_docs();
					mode = 0;
				}
                couch->handle_request(line);
			}
			else
                break;

		}

		delete couch;
	}
	return 0;
}
