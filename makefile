CC = g++

JSON_CPP_HOME=/home/nbarker/jsoncpp/jsoncpp-src-0.5.0

INCLUDES = -I$(JSON_CPP_HOME)/include

CFLAGS = -DXP_UNIX $(INCLUDES)

SRC = src

ARCHIVES = -lclucene-core -lclucene-shared -lcurl -ljs -ljsoncpp 

fti:  fti.o
	$(CC) $(CFLAGS) -dy -o fti fti.o couch_lucene.o $(ARCHIVES)

fti.o: $(SRC)/fti.cpp couch_lucene.o 
	$(CC) $(CFLAGS) -c $(SRC)/fti.cpp -o fti.o

couch_lucene.o: $(SRC)/couch_lucene.cpp $(SRC)/couch_lucene.h
	$(CC) $(CFLAGS) -c $(SRC)/couch_lucene.cpp -o couch_lucene.o

clean:
	rm *.o fti 
