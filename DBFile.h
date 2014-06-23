#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include <fstream>
//typedef enum {heap, sorted, tree} fType;
//typedef fType myType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

private:
	//Creating two instances of File and Page classes that can be used to store the filepath to be used and the page that has to be loaded.
	File fileBuffer; 
	Page pageBuffer;
	char *filename;
	static int havnop;
	//Current record pointer
	Record currentRecord;
	Schema *tempschema;
	ofstream myf;
	ifstream red;
	off_t pageno;//For current page number confusion
	//creating class dbfile Generic for internal working
	GenericDBFile *myInternalVar;
	
public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
